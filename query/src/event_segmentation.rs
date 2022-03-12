use super::error::Result;
use super::logical_plan::plan::LogicalPlan;
use crate::logical_plan::expr::{
    and, binary_expr, col, is_not_null, is_null, lit, lit_timestamp, or, Expr,
};
use chrono::{DateTime, Duration, Utc};
use datafusion::logical_plan::{Column, DFField, DFSchema, Operator};
use datafusion::physical_plan::aggregates::AggregateFunction as DFAggregateFunction;
use datafusion::scalar::ScalarValue as DFScalarValue;

use crate::physical_plan::expressions::partitioned_aggregate::PartitionedAggregateFunction;
use crate::Context;
use common::ScalarValue;
use futures::executor;
use metadata::Metadata;
use std::ops::Sub;
use std::sync::Arc;
use crate::physical_plan::expressions::aggregate::AggregateFunction;

pub mod event_fields {
    pub const EVENT: &str = "event";
    pub const CREATED_AT: &str = "created_at";
    pub const USER_ID: &str = "user_id";
}

#[derive(Clone)]
pub enum TimeUnit {
    Second,
    Minute,
    Hour,
    Day,
    Week,
    Month,
    Year,
}

impl TimeUnit {
    pub fn sub(&self, n: i64) -> DateTime<Utc> {
        match self {
            TimeUnit::Second => Utc::now().sub(Duration::seconds(n)),
            TimeUnit::Minute => Utc::now().sub(Duration::minutes(n)),
            TimeUnit::Hour => Utc::now().sub(Duration::hours(n)),
            TimeUnit::Day => Utc::now().sub(Duration::days(n)),
            TimeUnit::Week => Utc::now().sub(Duration::weeks(n)),
            TimeUnit::Month => Utc::now().sub(Duration::days(n) * 30),
            TimeUnit::Year => Utc::now().sub(Duration::days(n) * 365),
        }
    }
}

pub enum PropertyScope {
    Event,
    User,
}

#[derive(Clone)]
pub enum QueryTime {
    Between {
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    },
    From(DateTime<Utc>),
    Last {
        n: i64,
        unit: TimeUnit,
    },
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
pub enum Operation {
    Eq,
    Neq,
    IsNull,
    IsNotNull,
}

impl Into<Operator> for Operation {
    fn into(self) -> Operator {
        match self {
            Operation::Eq => Operator::Eq,
            Operation::Neq => Operator::NotEq,
            _ => panic!("unreachable"),
        }
    }
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
    pub fn aggregate_function(&self) -> DFAggregateFunction {
        match self {
            QueryAggregate::Min => DFAggregateFunction::Min,
            QueryAggregate::Max => DFAggregateFunction::Max,
            QueryAggregate::Sum => DFAggregateFunction::Sum,
            QueryAggregate::Avg => DFAggregateFunction::Avg,
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
pub enum PropertyRef {
    User(String),
    UserCustom(String),
    Event(String),
    EventCustom(String),
}

impl PropertyRef {
    pub fn name(&self) -> String {
        match self {
            PropertyRef::User(name) => name.clone(),
            PropertyRef::UserCustom(name) => name.clone(),
            PropertyRef::Event(name) => name.clone(),
            PropertyRef::EventCustom(name) => name.clone(),
        }
    }
}

#[derive(Clone)]
pub enum EventFilter {
    Property {
        property: PropertyRef,
        operation: Operation,
        value: Option<Vec<ScalarValue>>,
    },
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
        input: Arc<LogicalPlan>,
        es: EventSegmentation,
    ) -> Result<LogicalPlan> {
        let event = es.events[0].clone();
        let builder = LogicalPlanBuilder { ctx, metadata, es };

        builder.build_event_logical_plan(input, &event).await
    }

    async fn build_event_logical_plan(
        &self,
        input: Arc<LogicalPlan>,
        event: &Event,
    ) -> Result<LogicalPlan> {
        let filter = self.build_filter_logical_plan(input.clone(), event).await?;
        let agg = self
            .build_aggregate_logical_plan(Arc::new(filter), event)
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
        Ok(LogicalPlan::Filter {
            predicate: expr,
            input,
        })
    }

    // builds logical plan for aggregate
    async fn build_aggregate_logical_plan(
        &self,
        input: Arc<LogicalPlan>,
        event: &Event,
    ) -> Result<LogicalPlan> {
        let mut group_expr: Vec<Expr> = vec![];
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
                        Expr::AggregateFunction {
                            fun: AggregateFunction::SortedDistinctCount,
                            args: vec![col(self.es.group.as_ref())],
                            distinct: true,
                        }
                    }
                    Query::WeeklyActiveGroups => unimplemented!(),
                    Query::MonthlyActiveGroups => unimplemented!(),
                    Query::CountPerGroup { aggregate } => Expr::AggregatePartitionedFunction {
                        partition_by: Box::new(col(self.es.group.as_ref())),
                        fun: PartitionedAggregateFunction::Count,
                        outer_fun: aggregate.clone(),
                        args: vec![col(event_fields::USER_ID)],
                        distinct: false,
                    },
                    Query::AggregatePropertyPerGroup {
                        property,
                        aggregate_per_group,
                        aggregate,
                    } => Expr::AggregatePartitionedFunction {
                        partition_by: Box::new(col(self.es.group.as_ref())),
                        fun: aggregate_per_group.clone(),
                        outer_fun: aggregate.clone(),
                        args: vec![executor::block_on(self.property_col(property))?],
                        distinct: false,
                    },
                    Query::AggregateProperty {
                        property,
                        aggregate,
                    } => Expr::AggregateFunction {
                        fun: aggregate.clone(),
                        args: vec![executor::block_on(self.property_col(property))?],
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

        let expr = LogicalPlan::Aggregate {
            input,
            group_expr,
            aggr_expr,
            schema: Arc::new(aggr_schema),
        };

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
                    lit(DFScalarValue::from(e.id)),
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
        // vector of expression for OR
        let filter_exprs: Vec<Expr> = vec![];

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
                    } => executor::block_on(self.property_expression(property, operation, value)),
                }
            })
            .collect::<Result<Vec<Expr>>>()?;

        if filters_exprs.len() == 1 {
            return Ok(filter_exprs[0].clone());
        }

        Ok(multi_and(filters_exprs))
    }

    // builds breakdown expression
    async fn breakdown_expr(&self, breakdown: &Breakdown) -> Result<Expr> {
        match breakdown {
            Breakdown::Property(prop_ref) => match prop_ref {
                PropertyRef::User(prop_name) | PropertyRef::Event(prop_name) => {
                    let prop_col = self.property_col(&prop_ref).await?;
                    Ok(Expr::Alias(Box::new(prop_col), prop_name.clone()))
                }
                PropertyRef::UserCustom(_) => unimplemented!(),
                PropertyRef::EventCustom(_) => unimplemented!(),
            },
        }
    }

    /// builds name [property] [op] [value] expression
    pub async fn property_expression(
        &self,
        property: &PropertyRef,
        operation: &Operation,
        value: &Option<Vec<ScalarValue>>,
    ) -> Result<Expr> {
        match property {
            PropertyRef::User(_) | PropertyRef::Event(_) => {
                let prop_col = self.property_col(&property).await?;
                named_property_expression(prop_col, operation, value)
            }
            PropertyRef::UserCustom(_) => unimplemented!(),
            PropertyRef::EventCustom(_) => unimplemented!(),
        }
    }

    pub async fn property_col(&self, property: &PropertyRef) -> Result<Expr> {
        Ok(match property {
            PropertyRef::User(prop_name) => {
                let prop = self
                    .metadata
                    .user_properties
                    .get_by_name(self.ctx.organization_id, self.ctx.project_id, prop_name)
                    .await?;
                col(prop.col_id.to_string().as_str())
            }
            PropertyRef::UserCustom(_prop_name) => unimplemented!(),
            PropertyRef::Event(prop_name) => {
                let prop = self
                    .metadata
                    .event_properties
                    .get_by_name(self.ctx.organization_id, self.ctx.project_id, prop_name)
                    .await?;
                col(prop.col_id.to_string().as_str())
            }
            PropertyRef::EventCustom(_) => unimplemented!(),
        })
    }
}

fn multi_or(exprs: Vec<Expr>) -> Expr {
    // combine multiple values with OR
    // create initial OR between two first expressions
    let mut expr = or(exprs[0].clone(), exprs[1].clone());
    // iterate over rest of expression (3rd and so on) and add them to the final expression
    for vexpr in exprs.iter().skip(2) {
        // wrap into OR
        expr = or(expr.clone(), vexpr.clone());
    }

    expr
}

fn multi_and(exprs: Vec<Expr>) -> Expr {
    let mut expr = and(exprs[0].clone(), exprs[1].clone());
    for fexpr in exprs.iter().skip(2) {
        expr = and(expr.clone(), fexpr.clone())
    }

    expr
}

/// builds "[property] [op] [values]" binary expression with already known property column
fn named_property_expression(
    prop_col: Expr,
    operation: &Operation,
    values: &Option<Vec<ScalarValue>>,
) -> Result<Expr> {
    match operation {
        Operation::Eq | Operation::Neq => {
            // expressions for OR
            let mut exprs: Vec<Expr> = vec![];

            let values_vec = values.as_ref().unwrap();
            // iterate over all possible values
            for value in values_vec.into_iter() {
                exprs.push(binary_expr(
                    prop_col.clone(),
                    operation.clone().into(),
                    lit(value.clone().to_df()),
                ));
            }

            // for only one value we just return first expression
            if values_vec.len() == 1 {
                return Ok(exprs[0].clone());
            }

            Ok(multi_or(exprs))
        }
        // for isNull and isNotNull we don't need values at all
        Operation::IsNull => Ok(is_null(prop_col)),
        Operation::IsNotNull => Ok(is_not_null(prop_col)),
    }
}

/// builds expression on timestamp
fn time_expression(time: &QueryTime) -> Expr {
    let ts_col = Expr::Column(Column::from_qualified_name(event_fields::CREATED_AT));
    match time {
        QueryTime::Between { from, to } => {
            let left = binary_expr(
                ts_col.clone(),
                Operator::GtEq,
                lit_timestamp(from.timestamp_nanos() / 1_000),
            );

            let right = binary_expr(
                ts_col,
                Operator::LtEq,
                lit_timestamp(to.timestamp_nanos() / 1_000),
            );

            and(left, right)
        }
        QueryTime::From(from) => binary_expr(
            ts_col,
            Operator::GtEq,
            lit_timestamp(from.timestamp_nanos() / 1_000),
        ),
        QueryTime::Last { n: last, unit } => {
            let from = unit.sub(*last);
            binary_expr(ts_col, Operator::GtEq, lit_timestamp(from.timestamp()))
        }
    }
}

/// Create field meta-data from an expression, to use in a result set schema
pub fn exprlist_to_fields<'a>(
    expr: impl IntoIterator<Item=&'a Expr>,
    input_schema: &DFSchema,
) -> Result<Vec<DFField>> {
    expr.into_iter().map(|e| e.to_field(input_schema)).collect()
}
