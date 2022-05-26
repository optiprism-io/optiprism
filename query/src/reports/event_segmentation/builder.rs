use crate::error::{Error, Result};
use chrono::{DateTime, Duration, Utc};
use datafusion::logical_plan::{
    create_udaf, exprlist_to_fields, Column, DFField, DFSchema, ExprSchema, LogicalPlan, Operator,
};
use datafusion::physical_plan::aggregates::{return_type, AggregateFunction};

use crate::reports::types::{PropValueOperation, PropertyRef, QueryTime, TimeUnit, EventRef};
use crate::logical_plan::expr::{aggregate_partitioned, multi_and, sorted_distinct_count};
use crate::physical_plan::expressions::aggregate::state_types;
use crate::physical_plan::expressions::partitioned_aggregate::{
    PartitionedAggregate, PartitionedAggregateFunction,
};
use crate::physical_plan::expressions::sorted_distinct_count::SortedDistinctCount;
use crate::{event_fields, Context};
use arrow::datatypes::DataType;
use axum::response::IntoResponse;
use datafusion::error::Result as DFResult;
use datafusion::logical_plan::plan::{Aggregate, Extension, Filter};
use datafusion::logical_plan::ExprSchemable;
use datafusion_expr::expr_fn::{and, binary_expr, or};
use datafusion_expr::{
    col, lit, AccumulatorFunctionImplementation, AggregateUDF, BuiltinScalarFunction, Expr,
    ReturnTypeFunction, Signature, StateTypeFunction, Volatility,
};

use std::io::{self, Write};
use std::ops::{Add, Sub};
use futures::executor;
use futures::executor::block_on;
use metadata::properties::provider::Namespace;
use metadata::Metadata;
use std::sync::Arc;
use crate::logical_plan::merge::MergeNode;
use crate::logical_plan::pivot::PivotNode;
use crate::logical_plan::unpivot::UnpivotNode;
use crate::physical_plan::unpivot::UnpivotExec;
use serde::{Deserialize, Serialize};
use datafusion_common::ScalarValue;

use crate::reports::event_segmentation::types::{Breakdown, Event, EventFilter, EventSegmentation, Query};
use crate::reports::expr::{property_col, property_expression, time_expression};

const COL_NAME: &str = "name";
const COL_VALUE: &str = "value";
const COL_EVENT: &str = "event";
const COL_DATE: &str = "date";

pub struct LogicalPlanBuilder {
    ctx: Context,
    cur_time: DateTime<Utc>,
    metadata: Arc<Metadata>,
    es: EventSegmentation,
}

impl LogicalPlanBuilder {
    /// creates logical plan for event segmentation
    pub async fn build(
        ctx: Context,
        cur_time: DateTime<Utc>,
        metadata: Arc<Metadata>,
        input: LogicalPlan,
        es: EventSegmentation,
    ) -> Result<LogicalPlan> {
        let events = es.events.clone();
        let builder = LogicalPlanBuilder { ctx, cur_time, metadata, es: es.clone() };

        // build main query
        let mut input = match events.len() {
            1 => builder.build_event_logical_plan(input.clone(), 0).await?,
            _ => {
                let mut inputs: Vec<LogicalPlan> = vec![];
                for idx in 0..events.len() {
                    let input = builder.build_event_logical_plan(input.clone(), idx).await?;

                    inputs.push(input);
                }

                // merge multiple results into one schema
                LogicalPlan::Extension(Extension {
                    node: Arc::new(MergeNode::try_new(inputs).map_err(|e| e.into_datafusion_plan_error())?)
                })
            }
        };

        // unpivot aggregate values into value column
        input = {
            let agg_cols = events
                .iter()
                .flat_map(|e| {
                    e.queries
                        .iter()
                        .map(|q| q.clone().name.unwrap())
                })
                .collect();

            LogicalPlan::Extension(Extension {
                node: Arc::new(UnpivotNode::try_new(
                    input,
                    agg_cols,
                    COL_NAME.to_string(),
                    COL_VALUE.to_string(),
                )?),
            })
        };

        // pivot date
        input = {
            let result_cols = es.time_columns(cur_time);
            LogicalPlan::Extension(Extension {
                node: Arc::new(PivotNode::try_new(
                    input,
                    Column::from_name(COL_DATE),
                    Column::from_name(COL_VALUE),
                    result_cols,
                )?),
            })
        };

        Ok(input)
    }

    async fn build_event_logical_plan(
        &self,
        input: LogicalPlan,
        event_id: usize,
    ) -> Result<LogicalPlan> {
        let filter = self.build_filter_logical_plan(input.clone(), &self.es.events[event_id]).await?;
        let agg = self
            .build_aggregate_logical_plan(filter, &self.es.events[event_id])
            .await?;
        Ok(agg)
    }

    /// builds filter plan
    async fn build_filter_logical_plan(
        &self,
        input: LogicalPlan,
        event: &Event,
    ) -> Result<LogicalPlan> {
        // time filter
        let ts_col = Expr::Column(Column::from_qualified_name(event_fields::CREATED_AT));
        let mut expr = time_expression(ts_col, input.schema(), &self.es.time, &self.cur_time)?;

        // event filter (event name, properties)
        expr = and(expr, self.event_expression(event).await?);

        // global event filters
        if let Some(filters) = &self.es.filters {
            match &event.event {
                EventRef::Regular(_) => {
                    expr = and(expr.clone(), self.event_filters_expression(filters).await?);
                }
                EventRef::Custom(_) => unimplemented!(),
            }
        }

        //global filter
        Ok(LogicalPlan::Filter(Filter {
            predicate: expr,
            input: Arc::new(input),
        }))
    }

    // builds logical plan for aggregate
    async fn build_aggregate_logical_plan(
        &self,
        input: LogicalPlan,
        event: &Event,
    ) -> Result<LogicalPlan> {
        let mut group_expr: Vec<Expr> = vec![];

        let time_granularity = match self.es.interval_unit {
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
            args: vec![lit(time_granularity), ts_col],
        };

        group_expr.push(Expr::Alias(Box::new(lit(event.event.name())), COL_EVENT.to_string()));
        group_expr.push(Expr::Alias(Box::new(time_expr), COL_DATE.to_string()));

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
                        PartitionedAggregateFunction::Count,
                        aggregate.clone(),
                        vec![col(self.es.group.as_ref())],
                    )?,
                    Query::AggregatePropertyPerGroup {
                        property,
                        aggregate_per_group,
                        aggregate,
                    } => aggregate_partitioned(
                        input.schema(),
                        col(self.es.group.as_ref()),
                        aggregate_per_group.clone(),
                        aggregate.clone(),
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
            input: Arc::new(input),
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
                    lit(ScalarValue::from(e.id)),
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
                        let df_value: Option<Vec<ScalarValue>> = value
                            .as_ref()
                            .map(|x| x.iter().map(|s| s.clone()).collect());
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
                PropertyRef::Custom(_) => unimplemented!(),
            },
        }
    }
}

impl EventSegmentation {
    pub fn time_columns(&self, cur_time: DateTime<Utc>) -> Vec<String> {
        let (mut from, to) = match &self.time {
            QueryTime::Between { from, to } => (from.clone(), to.clone()),
            QueryTime::From(from) => (from.clone(), cur_time.clone()),
            QueryTime::Last { last, unit } => (cur_time.sub(unit.duration(*last)), cur_time.clone())
        };

        let mut result_cols: Vec<String> = vec![];
        let dur = self.interval_unit.duration(1);

        while from <= to {
            result_cols.push(from.naive_utc().to_string());
            from = from.add(dur.clone());
        }

        result_cols
    }
}