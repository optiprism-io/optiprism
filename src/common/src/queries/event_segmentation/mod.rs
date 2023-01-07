use chrono::DateTime;
use chrono::Utc;
use datafusion_expr::AggregateFunction;

pub mod logical_plan_builder;

use crate::physical_plan::expressions::partitioned_aggregate::PartitionedAggregateFunction;
use crate::queries::QueryTime;
use crate::queries::TimeIntervalUnit;
use crate::types::EventFilter;
use crate::types::EventRef;
use crate::types::PropertyRef;

#[derive(Clone, Debug)]
pub enum SegmentTime {
    Between {
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    },
    From(DateTime<Utc>),
    Last {
        n: i64,
        unit: TimeIntervalUnit,
    },
    AfterFirstUse {
        within: i64,
        unit: TimeIntervalUnit,
    },
    WindowEach {
        unit: TimeIntervalUnit,
    },
}

#[derive(Clone, Debug)]
pub enum ChartType {
    Line,
    Bar,
}

#[derive(Clone, Debug)]
pub enum Analysis {
    Linear,
    RollingAverage {
        window: usize,
        unit: TimeIntervalUnit,
    },
    Logarithmic,
    Cumulative,
}

#[derive(Clone, Debug)]
pub struct Compare {
    pub offset: usize,
    pub unit: TimeIntervalUnit,
}

#[derive(Clone, Debug)]
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

#[derive(Clone, Debug)]
pub enum QueryAggregatePerGroup {
    Min,
    Max,
    Sum,
    Avg,
    Median,
    DistinctCount,
}

#[derive(Clone, Debug)]
pub enum QueryPerGroup {
    CountEvents,
}

#[derive(Clone, Debug)]
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

#[derive(Clone, Debug)]
pub struct NamedQuery {
    pub agg: Query,
    pub name: Option<String>,
}

impl NamedQuery {
    pub fn new(agg: Query, name: Option<String>) -> Self {
        NamedQuery { name, agg }
    }
}

#[derive(Clone, Debug)]
pub enum Breakdown {
    Property(PropertyRef),
}

#[derive(Clone, Debug)]
pub struct Event {
    pub event: EventRef,
    pub filters: Option<Vec<EventFilter>>,
    pub breakdowns: Option<Vec<Breakdown>>,
    pub queries: Vec<NamedQuery>,
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

#[derive(Clone, Debug)]
pub struct EventSegmentation {
    pub time: QueryTime,
    pub group: String,
    pub interval_unit: TimeIntervalUnit,
    pub chart_type: ChartType,
    pub analysis: Analysis,
    pub compare: Option<Compare>,
    pub events: Vec<Event>,
    pub filters: Option<Vec<EventFilter>>,
    pub breakdowns: Option<Vec<Breakdown>>,
}
