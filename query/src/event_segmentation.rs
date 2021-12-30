use super::error::Result;
use super::logical_plan::plan::LogicalPlan;
use crate::logical_plan::expr::{
    and, binary_expr, col, is_not_null, is_null, lit, lit_timestamp, or, Expr,
};
use chrono::{DateTime, Duration, Utc};
use datafusion::datasource::TableProvider;
use datafusion::logical_plan::{Column, DFField, DFSchema, Operator};
use datafusion::physical_plan::aggregates::AggregateFunction;
use datafusion::scalar::ScalarValue;

use std::ops::Sub;
use std::sync::Arc;
use store::dictionary::DictionaryProvider;
use store::schema::{event_fields, SchemaProvider};

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
        aggregate_per_group: AggregateFunction,
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
        NamedQuery {
            name: name.clone(),
            agg: agg.clone(),
        }
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
pub enum Value {
    Int8(i8),
    Int16(i16),
    Float64(f64),
    Boolean(bool),
    Utf8(String),
}

#[derive(Clone)]
pub enum EventFilter {
    Property {
        property: PropertyRef,
        operation: Operation,
        value: Option<Vec<Value>>,
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
    time: QueryTime,
    group: String,
    interval_unit: TimeUnit,
    chart_type: ChartType,
    analysis: Analysis,
    compare: Option<Compare>,
    events: Vec<Event>,
    filters: Option<Vec<EventFilter>>,
    breakdowns: Option<Vec<Breakdown>>,
    segments: Option<Vec<Segment>>,
}

pub fn validate(_es: &EventSegmentation) -> Result<()> {
    Ok(())
}

pub fn events_projection(_es: &EventSegmentation) -> Option<Vec<usize>> {
    Some(vec![0, 1, 2])
}

fn named_property_expression(
    col_name: &str,
    operation: &Operation,
    values: &Option<Vec<Value>>,
) -> Expr {
    let prop_col = col(col_name);

    match operation {
        Operation::Eq | Operation::Neq => {
            // expressions for OR
            let mut exprs: Vec<Expr> = vec![];

            let values_vac = values.as_ref().unwrap();
            // iterate over all possible values
            for value in values_vac.iter() {
                let sv = match value {
                    Value::Int8(v) => ScalarValue::from(*v),
                    Value::Int16(v) => ScalarValue::from(*v),
                    Value::Float64(v) => ScalarValue::from(*v),
                    Value::Boolean(v) => ScalarValue::from(*v),
                    Value::Utf8(v) => ScalarValue::from(v.as_str()),
                };
                exprs.push(binary_expr(
                    prop_col.clone(),
                    operation.clone().into(),
                    lit(sv),
                ));
            }

            // for only one value we just return first expression
            if values_vac.len() == 1 {
                return exprs[0].clone();
            }

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
        Operation::IsNull => is_null(prop_col.clone()),
        Operation::IsNotNull => is_not_null(prop_col.clone()),
    }
}

fn property_db_col_name(
    schema: Arc<dyn SchemaProvider>,
    event_name: &str,
    property: &PropertyRef,
) -> String {
    match property {
        PropertyRef::User(prop_name) => {
            let prop = schema.get_user_property_by_name(prop_name).unwrap();
            prop.db_col_name()
        }
        PropertyRef::UserCustom(_) => unimplemented!(),
        PropertyRef::Event(prop_name) => {
            let prop = schema
                .get_event_property_by_name(event_name, prop_name)
                .unwrap();
            prop.db_col_name()
        }
        PropertyRef::EventCustom(_) => unimplemented!(),
    }
}

// name prop_name op value expression
pub fn property_expression(
    schema: Arc<dyn SchemaProvider>,
    event_name: &str,
    property: &PropertyRef,
    operation: &Operation,
    value: &Option<Vec<Value>>,
) -> Expr {
    match property {
        PropertyRef::User(prop_name) => {
            let prop = schema.get_user_property_by_name(prop_name).unwrap();
            let col_name = prop.db_col_name();
            named_property_expression(&col_name, operation, value)
        }
        PropertyRef::UserCustom(_prop_name) => unimplemented!(),
        PropertyRef::Event(prop_name) => {
            let prop = schema
                .get_event_property_by_name(event_name, prop_name)
                .unwrap();
            let col_name = prop.db_col_name();
            named_property_expression(&col_name, operation, value)
        }
        PropertyRef::EventCustom(_) => unimplemented!(),
    }
}

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

fn event_filters_expression(
    schema: Arc<dyn SchemaProvider>,
    event_name: &str,
    filters: &Vec<EventFilter>,
) -> Expr {
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
                } => property_expression(schema.clone(), event_name, property, operation, value),
            }
        })
        .collect::<Vec<Expr>>();

    if filters_exprs.len() == 1 {
        return filter_exprs[0].clone();
    } else {
        let mut expr = and(filters_exprs[0].clone(), filters_exprs[1].clone());
        for fexpr in filter_exprs.iter().skip(2) {
            expr = and(expr.clone(), fexpr.clone())
        }

        expr
    }
}

fn regular_event_expression(
    schema: Arc<dyn SchemaProvider>,
    dict_provider: Arc<dyn DictionaryProvider>,
    event_name: &str,
    event: &Event,
) -> Result<Expr> {
    // add event name condition
    let mut expr = binary_expr(
        col(event_fields::EVENT_NAME),
        Operator::Eq,
        lit(dict_provider.get_u16_by_key("events", event_name)?),
    );

    // apply filters
    if let Some(filters) = &event.filters {
        expr = and(
            expr.clone(),
            event_filters_expression(schema.clone(), event_name, filters),
        )
    }

    Ok(expr)
}

fn event_expression(
    schema: Arc<dyn SchemaProvider>,
    dict_provider: Arc<dyn DictionaryProvider>,
    event: &Event,
) -> Result<Expr> {
    // match event type
    match &event.event {
        // regular event
        EventRef::Regular(event_name) => {
            regular_event_expression(schema.clone(), dict_provider, event_name, event)
        }

        EventRef::Custom(_event_name) => unimplemented!(),
    }
}

fn plan_filter(
    input: Arc<LogicalPlan>,
    es: &EventSegmentation,
    schema: Arc<dyn SchemaProvider>,
    dict_provider: Arc<dyn DictionaryProvider>,
    event: &Event,
) -> Result<LogicalPlan> {
    // time filter
    let mut expr = time_expression(&es.time);

    // event filter (event name, properties)
    expr = and(
        expr,
        event_expression(schema.clone(), dict_provider.clone(), event)?,
    );

    // global event filters
    if let Some(filters) = &es.filters {
        match &event.event {
            EventRef::Regular(event_name) => {
                expr = and(
                    expr.clone(),
                    event_filters_expression(schema.clone(), &event_name, filters),
                );
            }
            EventRef::Custom(_) => unimplemented!(),
        }
    }

    //global filter
    Ok(LogicalPlan::Filter {
        predicate: expr,
        input: input.clone(),
    })
}

/// Create field meta-data from an expression, for use in a result set schema
pub fn exprlist_to_fields<'a>(
    expr: impl IntoIterator<Item = &'a Expr>,
    input_schema: &DFSchema,
) -> Result<Vec<DFField>> {
    expr.into_iter().map(|e| e.to_field(input_schema)).collect()
}

fn breakdown_expr(
    schema: Arc<dyn SchemaProvider>,
    event: &Event,
    breakdown: &Breakdown,
) -> Result<Expr> {
    match breakdown {
        Breakdown::Property(prop_ref) => match prop_ref {
            PropertyRef::User(prop_name) => {
                let prop = schema.get_user_property_by_name(prop_name).unwrap();
                let col_name = prop.db_col_name();
                return Ok(Expr::Alias(Box::new(col(&col_name)), prop.name));
            }
            PropertyRef::UserCustom(_) => unimplemented!(),
            PropertyRef::Event(prop_name) => {
                let prop = schema
                    .get_event_property_by_name(event.event.name().as_ref(), prop_name)
                    .unwrap();
                let col_name = prop.db_col_name();
                return Ok(Expr::Alias(Box::new(col(&col_name)), prop.name));
            }
            PropertyRef::EventCustom(_) => unimplemented!(),
        },
    }
}

fn plan_agg(
    input: Arc<LogicalPlan>,
    es: &EventSegmentation,
    schema: Arc<dyn SchemaProvider>,
    event: &Event,
) -> Result<LogicalPlan> {
    let mut group_expr: Vec<Expr> = vec![];
    // event groups
    if let Some(breakdowns) = &event.breakdowns {
        for breakdown in breakdowns.iter() {
            group_expr.push(breakdown_expr(schema.clone(), event, breakdown)?);
        }
    }

    // common groups
    if let Some(breakdowns) = &es.breakdowns {
        for breakdown in breakdowns.iter() {
            group_expr.push(breakdown_expr(schema.clone(), event, breakdown)?);
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
                    args: vec![col(event_fields::EVENT_NAME)],
                    distinct: false,
                },
                Query::CountUniqueGroups | Query::DailyActiveGroups => Expr::AggregateFunction {
                    fun: AggregateFunction::Count,
                    args: vec![col(es.group.as_ref())],
                    distinct: true,
                },
                Query::WeeklyActiveGroups => unimplemented!(),
                Query::MonthlyActiveGroups => unimplemented!(),
                Query::CountPerGroup { aggregate } => Expr::AggregatePartitionedFunction {
                    partition_by: Box::new(col(es.group.as_ref())),
                    fun: AggregateFunction::Count,
                    outer_fun: aggregate.clone(),
                    args: vec![col(event_fields::USER_ID)],
                    distinct: false,
                },
                Query::AggregatePropertyPerGroup {
                    property,
                    aggregate_per_group,
                    aggregate,
                } => Expr::AggregatePartitionedFunction {
                    partition_by: Box::new(col(es.group.as_ref())),
                    fun: aggregate_per_group.clone(),
                    outer_fun: aggregate.clone(),
                    args: vec![col(property_db_col_name(
                        schema.clone(),
                        event.event.name().as_ref(),
                        property,
                    )
                    .as_ref())],
                    distinct: false,
                },
                Query::AggregateProperty {
                    property,
                    aggregate,
                } => Expr::AggregateFunction {
                    fun: aggregate.clone(),
                    args: vec![col(property_db_col_name(
                        schema.clone(),
                        event.event.name().as_ref(),
                        property,
                    )
                    .as_ref())],
                    distinct: false,
                },
                Query::QueryFormula { .. } => unimplemented!(),
            };

            match &query.name {
                None => Expr::Alias(Box::new(q), format!("agg_{}", id)),
                Some(name) => Expr::Alias(Box::new(q), name.clone()),
            }
        })
        .collect::<Vec<Expr>>();

    // TODO: create own normalizer or get rid normalizer at all
    // let group_expr = normalize_cols(group_expr, &input.to_df_logical_plan())?;
    // let aggr_expr = normalize_cols(aggr_expr, &input.to_df_logical_plan())?;
    let all_expr = group_expr.iter().chain(aggr_expr.iter());

    let aggr_schema = DFSchema::new(exprlist_to_fields(all_expr, input.schema())?)?;

    let expr = LogicalPlan::Aggregate {
        input: input.clone(),
        group_expr,
        aggr_expr,
        schema: Arc::new(aggr_schema),
    };

    Ok(expr)
}

pub fn create_event_logical_plan(
    input: Arc<LogicalPlan>,
    es: &EventSegmentation,
    schema: Arc<dyn SchemaProvider>,
    dict_provider: Arc<dyn DictionaryProvider>,
    event: &Event,
) -> Result<LogicalPlan> {
    let filter = plan_filter(input.clone(), es, schema.clone(), dict_provider, event)?;
    let agg = plan_agg(Arc::new(filter), es, schema.clone(), event)?;
    Ok(agg)
}

#[cfg(test)]
mod tests {
    use crate::error::Result;
    use crate::event_segmentation::{
        create_event_logical_plan, Analysis, Breakdown, ChartType, Event, EventFilter, EventRef,
        EventSegmentation, NamedQuery, Operation, PropertyRef, Query, QueryTime, TimeUnit, Value,
    };
    use crate::logical_plan::expr::Expr;

    use crate::logical_plan::plan::LogicalPlan;
    use chrono::{DateTime, Duration, Utc};
    use datafusion::arrow::array::{
        Float64Array, Int32Array, Int8Array, StringArray, TimestampMicrosecondArray, UInt16Array,
        UInt64Array,
    };

    use datafusion::arrow::datatypes::*;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::arrow::util::pretty::print_batches;
    use datafusion::datasource::object_store::local::LocalFileSystem;
    use datafusion::datasource::{MemTable, TableProvider};
    use datafusion::execution::context::ExecutionContextState;
    use datafusion::logical_plan::LogicalPlan as DFLogicalPlan;
    use datafusion::logical_plan::LogicalPlanBuilder;
    use datafusion::physical_plan::planner::DefaultPhysicalPlanner;
    use datafusion::physical_plan::{aggregates, collect, PhysicalPlanner};
    use datafusion::prelude::CsvReadOptions;

    use std::ops::Sub;
    use std::sync::Arc;
    use store::dictionary::{DictionaryProvider, MockDictionary};
    use store::schema::{event_fields, DBCol, EventPropertyStatus, MockSchema};

    fn users_provider() -> Result<MemTable> {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "created_at",
                DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Microsecond, None),
                false,
            ),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
            Field::new("d", DataType::Int32, true),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampMicrosecondArray::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![4, 5, 6])),
                Arc::new(Int32Array::from(vec![7, 8, 9])),
                Arc::new(Int32Array::from(vec![None, None, Some(9)])),
            ],
        )?;

        Ok(MemTable::try_new(schema, vec![vec![batch]])?)
    }

    fn events_schema() -> Schema {
        Schema::new(vec![
            Field::new(event_fields::USER_ID, DataType::UInt64, false),
            Field::new(
                event_fields::CREATED_AT,
                DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Microsecond, None),
                false,
            ),
            Field::new(event_fields::EVENT_NAME, DataType::UInt16, false),
            Field::new("country", DataType::Utf8, true),
            Field::new("device", DataType::Utf8, true),
            Field::new("0_float64", DataType::Float64, true),
            Field::new("1_utf8", DataType::Utf8, true),
            Field::new("2_int8", DataType::Int8, true),
        ])
    }

    async fn events_provider() -> Result<LogicalPlan> {
        let schema = Arc::new(events_schema());
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(vec![0u64; 0])),
                Arc::new(TimestampMicrosecondArray::from(vec![0i64; 0])),
                Arc::new(UInt16Array::from(vec![0u16; 0])),
                Arc::new(StringArray::from(vec!["".to_string(); 0])),
                Arc::new(StringArray::from(vec!["".to_string(); 0])),
                Arc::new(Float64Array::from(vec![0f64; 0])),
                Arc::new(StringArray::from(vec!["".to_string(); 0])),
                Arc::new(Int8Array::from(vec![0i8; 0])),
            ],
        )?;

        let _prov = MemTable::try_new(schema.clone(), vec![vec![batch]])?;
        let path = "/Users/ravlio/work/rust/exprtree/tests/events.csv";

        let schema = events_schema();
        let options = CsvReadOptions::new().schema(&schema);
        let df_input =
            LogicalPlanBuilder::scan_csv(Arc::new(LocalFileSystem {}), path, options, None, 1)
                .await?;

        Ok(match df_input.build()? {
            DFLogicalPlan::TableScan {
                table_name,
                source,
                projection,
                projected_schema,
                filters,
                limit,
            } => LogicalPlan::TableScan {
                table_name,
                source,
                projection,
                projected_schema,
                filters: filters.iter().map(|e| Expr::from_df_expr(e)).collect(),
                limit,
            },
            _ => unreachable!(),
        })
    }

    fn create_schema_mock() -> MockSchema {
        fn get_event_by_name(event_name: &str) -> store::error::Result<store::schema::Event> {
            match event_name {
                "View Product" => Ok(store::schema::Event {
                    id: 1,
                    created_at: Utc::now(),
                    updated_at: None,
                    created_by: 0,
                    updated_by: 0,
                    project_id: 0,
                    is_system: false,
                    tags: vec![],
                    name: "View Product".to_string(),
                    description: "".to_string(),
                    status: store::schema::EventStatus::Enabled,
                    properties: None,
                }),
                "Buy Product" => Ok(store::schema::Event {
                    id: 2,
                    created_at: Utc::now(),
                    updated_at: None,
                    created_by: 0,
                    updated_by: 0,
                    project_id: 0,
                    is_system: false,
                    tags: vec![],
                    name: "Buy Product".to_string(),
                    description: "".to_string(),
                    status: store::schema::EventStatus::Enabled,
                    properties: None,
                }),
                _ => panic!(),
            }
        }

        fn get_event_property_by_name(
            event_name: &str,
            property_name: &str,
        ) -> store::error::Result<store::schema::EventProperty> {
            match (event_name, property_name) {
                ("Buy Product", "revenue") => Ok(store::schema::EventProperty {
                    id: 1,
                    created_at: Utc::now(),
                    updated_at: None,
                    created_by: 0,
                    updated_by: 0,
                    is_system: false,
                    is_global: true,
                    tags: vec![],
                    name: "revenue".to_string(),
                    description: "".to_string(),
                    display_name: "Revenue".to_string(),
                    typ: DataType::Float64,
                    db_col: DBCol::Order(0),
                    status: EventPropertyStatus::Enabled,
                    nullable: false,
                    is_array: false,
                    is_dictionary: false,
                    dictionary_type: None,
                }),
                ("Buy Product", "Product Name") => Ok(store::schema::EventProperty {
                    id: 2,
                    created_at: Utc::now(),
                    updated_at: None,
                    created_by: 0,
                    updated_by: 0,
                    is_system: false,
                    is_global: true,
                    tags: vec![],
                    name: "product name".to_string(),
                    description: "".to_string(),
                    display_name: "Product Name".to_string(),
                    typ: DataType::Utf8,
                    db_col: DBCol::Order(1),
                    status: EventPropertyStatus::Enabled,
                    nullable: false,
                    is_array: false,
                    is_dictionary: false,
                    dictionary_type: None,
                }),
                _ => panic!(),
            }
        }

        fn get_user_property_by_name(
            property_name: &str,
        ) -> store::error::Result<store::schema::UserProperty> {
            match property_name {
                "country" => Ok(store::schema::UserProperty {
                    id: 1,
                    schema_id: 0,
                    created_at: Utc::now(),
                    updated_at: None,
                    created_by: 0,
                    updated_by: 0,
                    is_system: false,
                    tags: vec![],
                    name: "country".to_string(),
                    description: "".to_string(),
                    typ: DataType::Utf8,
                    db_col: DBCol::Named("country".to_string()),
                    nullable: false,
                    is_array: false,
                    is_dictionary: false,
                    dictionary_type: None,
                }),
                "device" => Ok(store::schema::UserProperty {
                    id: 2,
                    schema_id: 0,
                    created_at: Utc::now(),
                    updated_at: None,
                    created_by: 0,
                    updated_by: 0,
                    is_system: false,
                    tags: vec![],
                    name: "device".to_string(),
                    description: "".to_string(),
                    typ: DataType::Utf8,
                    db_col: DBCol::Named("device".to_string()),
                    nullable: false,
                    is_array: false,
                    is_dictionary: false,
                    dictionary_type: None,
                }),
                "is_premium" => Ok(store::schema::UserProperty {
                    id: 3,
                    schema_id: 0,
                    created_at: Utc::now(),
                    updated_at: None,
                    created_by: 0,
                    updated_by: 0,
                    is_system: false,
                    tags: vec![],
                    name: "is_premium".to_string(),
                    description: "".to_string(),
                    typ: DataType::Int8,
                    db_col: DBCol::Order(2),
                    nullable: false,
                    is_array: false,
                    is_dictionary: false,
                    dictionary_type: None,
                }),
                _ => panic!(),
            }
        }
        let mut mock_schema = MockSchema::new();
        mock_schema.get_event_by_name = Some(get_event_by_name);
        mock_schema.get_event_property_by_name = Some(get_event_property_by_name);
        mock_schema.get_user_property_by_name = Some(get_user_property_by_name);

        mock_schema
    }

    #[tokio::test]
    async fn test_filters() -> Result<()> {
        let to = DateTime::parse_from_rfc3339("2021-09-08T15:42:29.190855+00:00")
            .unwrap()
            .with_timezone(&Utc);
        let es = EventSegmentation {
            time: QueryTime::Between {
                from: to.clone().sub(Duration::days(10)),
                to: to.clone(),
            },
            group: event_fields::USER_ID.to_string(),
            interval_unit: TimeUnit::Day,
            chart_type: ChartType::Line,
            analysis: Analysis::Linear,
            compare: None,
            events: vec![Event::new(
                EventRef::Regular("Buy Product".to_string()),
                Some(vec![
                    EventFilter::Property {
                        property: PropertyRef::Event("revenue".to_string()),
                        operation: Operation::IsNull,
                        value: None,
                    },
                    EventFilter::Property {
                        property: PropertyRef::Event("revenue".to_string()),
                        operation: Operation::Eq,
                        value: Some(vec![
                            Value::Float64(1.0),
                            Value::Float64(2.0),
                            Value::Float64(3.0),
                        ]),
                    },
                    EventFilter::Property {
                        property: PropertyRef::User("country".to_string()),
                        operation: Operation::IsNull,
                        value: None,
                    },
                    EventFilter::Property {
                        property: PropertyRef::User("country".to_string()),
                        operation: Operation::Eq,
                        value: Some(vec![
                            Value::Utf8("Spain".to_string()),
                            Value::Utf8("France".to_string()),
                        ]),
                    },
                ]),
                Some(vec![Breakdown::Property(PropertyRef::Event(
                    "Product Name".to_string(),
                ))]),
                vec![
                    NamedQuery::new(Query::CountEvents, Some("count".to_string())),
                    NamedQuery::new(
                        Query::CountUniqueGroups,
                        Some("count_unique_users".to_string()),
                    ),
                    NamedQuery::new(
                        Query::CountPerGroup {
                            aggregate: aggregates::AggregateFunction::Avg,
                        },
                        Some("count_per_user".to_string()),
                    ),
                    NamedQuery::new(
                        Query::AggregatePropertyPerGroup {
                            property: PropertyRef::Event("revenue".to_string()),
                            aggregate_per_group: aggregates::AggregateFunction::Avg,
                            aggregate: aggregates::AggregateFunction::Avg,
                        },
                        Some("avg_revenue_per_user".to_string()),
                    ),
                    NamedQuery::new(
                        Query::AggregatePropertyPerGroup {
                            property: PropertyRef::Event("revenue".to_string()),
                            aggregate_per_group: aggregates::AggregateFunction::Min,
                            aggregate: aggregates::AggregateFunction::Avg,
                        },
                        Some("min_revenue_per_user".to_string()),
                    ),
                    NamedQuery::new(
                        Query::AggregateProperty {
                            property: PropertyRef::Event("revenue".to_string()),
                            aggregate: aggregates::AggregateFunction::Sum,
                        },
                        Some("sum_revenue".to_string()),
                    ),
                ],
            )],
            filters: Some(vec![
                EventFilter::Property {
                    property: PropertyRef::User("device".to_string()),
                    operation: Operation::Eq,
                    value: Some(vec![Value::Utf8("Iphone".to_string())]),
                },
                EventFilter::Property {
                    property: PropertyRef::User("is_premium".to_string()),
                    operation: Operation::Eq,
                    value: Some(vec![Value::Int8(1)]),
                },
            ]),
            breakdowns: Some(vec![Breakdown::Property(PropertyRef::User(
                "device".to_string(),
            ))]),
            segments: None,
        };

        let schema = Arc::new(create_schema_mock());

        let mut dict_mock = MockDictionary::new();
        fn get_u16_by_key(_table: &str, _key: &str) -> store::error::Result<u16> {
            Ok(1)
        }
        dict_mock.get_u16_by_key = Some(get_u16_by_key);

        let plan = create_event_logical_plan(
            Arc::new(events_provider().await?),
            &es,
            schema.clone(),
            Arc::new(dict_mock),
            &es.events[0],
        )?;
        let df_plan = plan.to_df_plan()?;

        let mut ctx_state = ExecutionContextState::new();
        ctx_state.config.target_partitions = 1;
        let planner = DefaultPhysicalPlanner::default();
        let physical_plan = planner.create_physical_plan(&df_plan, &ctx_state).await?;

        let result = collect(physical_plan).await?;

        print_batches(&result)?;
        Ok(())
    }

    #[tokio::test]
    async fn test_query() -> Result<()> {
        let to = DateTime::parse_from_rfc3339("2021-09-08T15:42:29.190855+00:00")
            .unwrap()
            .with_timezone(&Utc);
        let es = EventSegmentation {
            time: QueryTime::Between {
                from: to.clone().sub(Duration::days(10)),
                to: to.clone(),
            },
            group: event_fields::USER_ID.to_string(),
            interval_unit: TimeUnit::Day,
            chart_type: ChartType::Line,
            analysis: Analysis::Linear,
            compare: None,
            events: vec![Event::new(
                EventRef::Regular("Buy Product".to_string()),
                None,
                None, //Some(vec![Breakdown::Property(PropertyRef::Event("Product Name".to_string()))]),
                vec![
                    NamedQuery::new(Query::CountEvents, Some("count".to_string())),
                    NamedQuery::new(
                        Query::CountUniqueGroups,
                        Some("count_unique_users".to_string()),
                    ),
                    NamedQuery::new(
                        Query::CountPerGroup {
                            aggregate: aggregates::AggregateFunction::Avg,
                        },
                        Some("count_per_user".to_string()),
                    ),
                    NamedQuery::new(
                        Query::AggregatePropertyPerGroup {
                            property: PropertyRef::Event("revenue".to_string()),
                            aggregate_per_group: aggregates::AggregateFunction::Sum,
                            aggregate: aggregates::AggregateFunction::Avg,
                        },
                        Some("avg_revenue_per_user".to_string()),
                    ),
                    NamedQuery::new(
                        Query::AggregateProperty {
                            property: PropertyRef::Event("revenue".to_string()),
                            aggregate: aggregates::AggregateFunction::Sum,
                        },
                        Some("sum_revenue".to_string()),
                    ),
                ],
            )],
            filters: None,
            breakdowns: Some(vec![Breakdown::Property(PropertyRef::User(
                "country".to_string(),
            ))]),
            segments: None,
        };

        let schema = Arc::new(create_schema_mock());

        let mut dict_mock = MockDictionary::new();
        fn get_u16_by_key(_: &str, key: &str) -> store::error::Result<u16> {
            Ok(match key {
                "View Product" => 1,
                "Buy Product" => 2,
                _ => panic!(),
            })
        }
        dict_mock.get_u16_by_key = Some(get_u16_by_key);

        let plan = create_event_logical_plan(
            Arc::new(events_provider().await?),
            &es,
            schema.clone(),
            Arc::new(dict_mock),
            &es.events[0],
        )?;
        let df_plan = plan.to_df_plan()?;

        let mut ctx_state = ExecutionContextState::new();
        ctx_state.config.target_partitions = 1;
        let planner = DefaultPhysicalPlanner::default();
        let physical_plan = planner.create_physical_plan(&df_plan, &ctx_state).await?;

        let result = collect(physical_plan).await?;

        print_batches(&result)?;
        Ok(())
    }
}
