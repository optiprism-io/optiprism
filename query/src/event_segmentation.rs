use super::error::{Error, Result};
use chrono::{DateTime, Duration, Utc};
use datafusion::logical_plan::{
    create_udaf, exprlist_to_fields, Column, DFField, DFSchema, LogicalPlan, Operator,
};
use datafusion::physical_plan::aggregates::{return_type, AggregateFunction};
use datafusion::scalar::ScalarValue as DFScalarValue;

use crate::common::{PropValueOperation, PropertyRef, QueryTime, TimeUnit};
use crate::logical_plan::expr::{aggregate_partitioned, multi_and, named_property_expression, property_col, property_expression, sorted_distinct_count, time_expression};
use crate::physical_plan::expressions::aggregate::state_types;
use crate::physical_plan::expressions::partitioned_aggregate::{
    PartitionedAggregate, PartitionedAggregateFunction,
};
use crate::physical_plan::expressions::sorted_distinct_count::SortedDistinctCount;
use crate::{event_fields, Context};
use arrow::datatypes::DataType;
use axum::response::IntoResponse;
use common::ScalarValue;
use datafusion::error::Result as DFResult;
use datafusion::logical_plan::plan::{Aggregate, Extension, Filter};
use datafusion::logical_plan::ExprSchemable;
use datafusion_expr::expr_fn::{and, binary_expr, or};
use datafusion_expr::{
    col, lit, AccumulatorFunctionImplementation, AggregateUDF, BuiltinScalarFunction, Expr,
    ReturnTypeFunction, Signature, StateTypeFunction, Volatility,
};
use futures::executor;
use futures::executor::block_on;
use metadata::properties::provider::Namespace;
use metadata::Metadata;
use std::ops::Sub;
use std::sync::Arc;
use crate::logical_plan::merge::MergeNode;

pub enum PropertyScope {
    Event,
    User,
}

pub enum SegmentTime {
    Between {
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    },
    From(DateTime<Utc>),
    Last {
        n: i64,
        unit: TimeUnit,
    },
    AfterFirstUse {
        within: i64,
        unit: TimeUnit,
    },
    WindowEach {
        unit: TimeUnit,
    },
}

pub enum ChartType {
    Line,
    Bar,
}

pub enum Analysis {
    Linear,
    RollingAverage { window: usize, unit: TimeUnit },
    WindowAverage { window: usize, unit: TimeUnit },
    Cumulative,
}

pub struct Compare {
    offset: usize,
    unit: TimeUnit,
}

#[derive(Clone)]
pub enum QueryAggregate {
    Min,
    Max,
    Sum,
    Avg,
    Median,
    DistinctCount,
    Percentile25th,
    Percentile75th,
    Percentile90th,
    Percentile99th,
}

impl QueryAggregate {
    pub fn aggregate_function(&self) -> AggregateFunction {
        match self {
            QueryAggregate::Min => AggregateFunction::Min,
            QueryAggregate::Max => AggregateFunction::Max,
            QueryAggregate::Sum => AggregateFunction::Sum,
            QueryAggregate::Avg => AggregateFunction::Avg,
            QueryAggregate::Median => unimplemented!(),
            QueryAggregate::DistinctCount => unimplemented!(),
            QueryAggregate::Percentile25th => unimplemented!(),
            QueryAggregate::Percentile75th => unimplemented!(),
            QueryAggregate::Percentile90th => unimplemented!(),
            QueryAggregate::Percentile99th => unimplemented!(),
        }
    }
}

#[derive(Clone)]
pub enum QueryAggregatePerGroup {
    Min,
    Max,
    Sum,
    Avg,
    Median,
    DistinctCount,
}

#[derive(Clone)]
pub enum QueryPerGroup {
    CountEvents,
}

#[derive(Clone)]
pub enum Query {
    CountEvents,
    CountUniqueGroups,
    DailyActiveGroups,
    WeeklyActiveGroups,
    MonthlyActiveGroups,
    CountPerGroup {
        aggregate: AggregateFunction,
    },
    AggregatePropertyPerGroup {
        property: PropertyRef,
        aggregate_per_group: PartitionedAggregateFunction,
        aggregate: AggregateFunction,
    },
    AggregateProperty {
        property: PropertyRef,
        aggregate: AggregateFunction,
    },
    QueryFormula {
        formula: String,
    },
}

#[derive(Clone)]
pub struct NamedQuery {
    agg: Query,
    name: Option<String>,
}

impl NamedQuery {
    pub fn new(agg: Query, name: Option<String>) -> Self {
        NamedQuery { name, agg }
    }
}

#[derive(Clone)]
pub enum EventFilter {
    Property {
        property: PropertyRef,
        operation: PropValueOperation,
        value: Option<Vec<ScalarValue>>,
    },
}

#[derive(Clone)]
pub enum Breakdown {
    Property(PropertyRef),
}

#[derive(Clone)]
pub struct Event {
    event: EventRef,
    filters: Option<Vec<EventFilter>>,
    breakdowns: Option<Vec<Breakdown>>,
    queries: Vec<NamedQuery>,
}

impl Event {
    pub fn new(
        event: EventRef,
        filters: Option<Vec<EventFilter>>,
        breakdowns: Option<Vec<Breakdown>>,
        queries: Vec<NamedQuery>,
    ) -> Self {
        Event {
            event,
            filters,
            breakdowns,
            queries,
        }
    }
}

pub enum SegmentCondition {}

pub struct Segment {
    name: String,
    conditions: Vec<SegmentCondition>,
}

pub struct EventSegmentation {
    pub time: QueryTime,
    pub group: String,
    pub interval_unit: TimeUnit,
    pub chart_type: ChartType,
    pub analysis: Analysis,
    pub compare: Option<Compare>,
    pub events: Vec<Event>,
    pub filters: Option<Vec<EventFilter>>,
    pub breakdowns: Option<Vec<Breakdown>>,
    pub segments: Option<Vec<Segment>>,
}

pub struct LogicalPlanBuilder {
    ctx: Context,
    metadata: Arc<Metadata>,
    es: EventSegmentation,
}

impl LogicalPlanBuilder {
    /// creates logical plan for event segmentation
    pub async fn build(
        ctx: Context,
        metadata: Arc<Metadata>,
        input: LogicalPlan,
        es: EventSegmentation,
    ) -> Result<LogicalPlan> {
        let len = es.events.len();
        let builder = LogicalPlanBuilder { ctx, metadata, es };

        Ok(match len {
            1 => builder.build_event_logical_plan(Arc::new(input.clone()), 0).await?,
            _ => {
                let mut inputs: Vec<LogicalPlan> = vec![];
                for idx in 0..len {
                    let input = builder.build_event_logical_plan(Arc::new(input.clone()), idx).await?;
                    inputs.push(input);
                }

                LogicalPlan::Extension(Extension {
                    node: Arc::new(MergeNode::try_new(inputs).map_err(|e| e.into_datafusion_plan_error())?)
                })
            }
        })
    }

    async fn build_event_logical_plan(
        &self,
        input: Arc<LogicalPlan>,
        event_id: usize,
    ) -> Result<LogicalPlan> {
        let filter = self.build_filter_logical_plan(input.clone(), &self.es.events[event_id]).await?;
        let agg = self
            .build_aggregate_logical_plan(Arc::new(filter), &self.es.events[event_id])
            .await?;
        Ok(agg)
    }

    /// builds filter plan
    async fn build_filter_logical_plan(
        &self,
        input: Arc<LogicalPlan>,
        event: &Event,
    ) -> Result<LogicalPlan> {
        // time filter
        let mut expr = time_expression(&self.es.time);

        // event filter (event name, properties)
        expr = and(expr, self.event_expression(event).await?);

        // global event filters
        if let Some(filters) = &self.es.filters {
            match &event.event {
                EventRef::Regular(event_name) => {
                    expr = and(expr.clone(), self.event_filters_expression(filters).await?);
                }
                EventRef::Custom(_) => unimplemented!(),
            }
        }

        //global filter
        Ok(LogicalPlan::Filter(Filter {
            predicate: expr,
            input,
        }))
    }

    // builds logical plan for aggregate
    async fn build_aggregate_logical_plan(
        &self,
        input: Arc<LogicalPlan>,
        event: &Event,
    ) -> Result<LogicalPlan> {
        let mut group_expr: Vec<Expr> = vec![];

        let time_gran = match self.es.interval_unit {
            TimeUnit::Second => "second",
            TimeUnit::Minute => "minute",
            TimeUnit::Hour => "hour",
            TimeUnit::Day => "day",
            TimeUnit::Week => "week",
            TimeUnit::Month => "month",
            TimeUnit::Year => "year",
        };

        let ts_col = Expr::Column(Column::from_qualified_name(event_fields::CREATED_AT));
        let time_expr = Expr::ScalarFunction {
            fun: BuiltinScalarFunction::DateTrunc,
            args: vec![lit(time_gran), ts_col],
        };

        group_expr.push(Expr::Alias(Box::new(lit(event.event.name())), "event".to_string()));
        group_expr.push(Expr::Alias(Box::new(time_expr), "date".to_string()));

        // event groups
        if let Some(breakdowns) = &event.breakdowns {
            for breakdown in breakdowns.iter() {
                group_expr.push(self.breakdown_expr(breakdown).await?);
            }
        }

        // common groups
        if let Some(breakdowns) = &self.es.breakdowns {
            for breakdown in breakdowns.iter() {
                group_expr.push(self.breakdown_expr(breakdown).await?);
            }
        }

        for (id, query) in event.queries.iter().enumerate() {}
        let aggr_expr = event
            .queries
            .iter()
            .enumerate()
            .map(|(id, query)| {
                let q = match &query.agg {
                    Query::CountEvents => Expr::AggregateFunction {
                        fun: AggregateFunction::Count,
                        args: vec![col(event_fields::EVENT)],
                        distinct: false,
                    },
                    Query::CountUniqueGroups | Query::DailyActiveGroups => {
                        let a = col(self.es.group.as_ref());
                        sorted_distinct_count(input.schema(), col(self.es.group.as_ref()))?
                    }
                    Query::WeeklyActiveGroups => unimplemented!(),
                    Query::MonthlyActiveGroups => unimplemented!(),
                    Query::CountPerGroup { aggregate } => aggregate_partitioned(
                        input.schema(),
                        col(self.es.group.as_ref()),
                        &PartitionedAggregateFunction::Count,
                        aggregate,
                        vec![col(self.es.group.as_ref())],
                    )?,
                    Query::AggregatePropertyPerGroup {
                        property,
                        aggregate_per_group,
                        aggregate,
                    } => aggregate_partitioned(
                        input.schema(),
                        col(self.es.group.as_ref()),
                        aggregate_per_group,
                        aggregate,
                        vec![executor::block_on(property_col(
                            &self.ctx,
                            &self.metadata,
                            property,
                        ))?],
                    )?,
                    Query::AggregateProperty {
                        property,
                        aggregate,
                    } => Expr::AggregateFunction {
                        fun: aggregate.clone(),
                        args: vec![executor::block_on(property_col(
                            &self.ctx,
                            &self.metadata,
                            property,
                        ))?],
                        distinct: false,
                    },
                    Query::QueryFormula { .. } => unimplemented!(),
                };

                match &query.name {
                    None => Ok(Expr::Alias(Box::new(q), format!("agg_{}", id))),
                    Some(name) => Ok(Expr::Alias(Box::new(q), name.clone())),
                }
            })
            .collect::<Result<Vec<Expr>>>()?;

        // todo check for duplicates
        let all_expr = group_expr.iter().chain(aggr_expr.iter());

        let aggr_schema = DFSchema::new(exprlist_to_fields(all_expr, input.schema())?)?;

        let expr = LogicalPlan::Aggregate(Aggregate {
            input,
            group_expr,
            aggr_expr,
            schema: Arc::new(aggr_schema),
        });

        Ok(expr)
    }

    /// builds expression for event
    async fn event_expression(&self, event: &Event) -> Result<Expr> {
        // match event type
        match &event.event {
            // regular event
            EventRef::Regular(_) => {
                let e = self
                    .metadata
                    .events
                    .get_by_name(
                        self.ctx.organization_id,
                        self.ctx.project_id,
                        event.event.name().as_str(),
                    )
                    .await?;
                // add event name condition
                let mut expr = binary_expr(
                    col(event_fields::EVENT),
                    Operator::Eq,
                    lit(DFScalarValue::from(e.id as u16)),
                );

                // apply filters
                if let Some(filters) = &event.filters {
                    expr = and(expr.clone(), self.event_filters_expression(filters).await?)
                }

                Ok(expr)
            }

            EventRef::Custom(_event_name) => unimplemented!(),
        }
    }

    /// builds event filters expression
    async fn event_filters_expression(&self, filters: &Vec<EventFilter>) -> Result<Expr> {
        // iterate over filters
        let filters_exprs = filters
            .iter()
            .map(|filter| {
                // match filter type
                match filter {
                    EventFilter::Property {
                        property,
                        operation,
                        value,
                    } => {
                        let df_value: Option<Vec<DFScalarValue>> = value
                            .as_ref()
                            .map(|x| x.iter().map(|s| s.clone().to_df()).collect());
                        executor::block_on(property_expression(
                            &self.ctx,
                            &self.metadata,
                            property,
                            operation,
                            df_value,
                        ))
                    }
                }
            })
            .collect::<Result<Vec<Expr>>>()?;

        if filters_exprs.len() == 1 {
            return Ok(filters_exprs[0].clone());
        }

        Ok(multi_and(filters_exprs))
    }

    // builds breakdown expression
    async fn breakdown_expr(&self, breakdown: &Breakdown) -> Result<Expr> {
        match breakdown {
            Breakdown::Property(prop_ref) => match prop_ref {
                PropertyRef::User(prop_name) | PropertyRef::Event(prop_name) => {
                    let prop_col = property_col(&self.ctx, &self.metadata, &prop_ref).await?;
                    Ok(Expr::Alias(Box::new(prop_col), prop_name.clone()))
                }
                PropertyRef::UserCustom(_) => unimplemented!(),
                PropertyRef::EventCustom(_) => unimplemented!(),
            },
        }
    }
}

#[derive(Clone)]
pub enum EventRef {
    Regular(String),
    Custom(String),
}

impl EventRef {
    pub fn name(&self) -> String {
        match self {
            EventRef::Regular(name) => name.clone(),
            EventRef::Custom(name) => name.clone(),
        }
    }
}
