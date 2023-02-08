use chrono::DateTime;
use chrono::Utc;
use serde::Deserialize;
use serde::Serialize;

use crate::query::time_columns;
use crate::query::AggregateFunction;
use crate::query::EventFilter;
use crate::query::EventRef;
use crate::query::PartitionedAggregateFunction;
use crate::query::PropertyRef;
use crate::query::QueryTime;
use crate::query::TimeIntervalUnit;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
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

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum ChartType {
    Line,
    Bar,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum Analysis {
    Linear,
    RollingAverage {
        window: usize,
        unit: TimeIntervalUnit,
    },
    Logarithmic,
    Cumulative,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Compare {
    pub offset: usize,
    pub unit: TimeIntervalUnit,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
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

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum QueryAggregatePerGroup {
    Min,
    Max,
    Sum,
    Avg,
    Median,
    DistinctCount,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum QueryPerGroup {
    CountEvents,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
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

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct NamedQuery {
    pub agg: Query,
    pub name: Option<String>,
}

impl NamedQuery {
    pub fn new(agg: Query, name: Option<String>) -> Self {
        NamedQuery { name, agg }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum Breakdown {
    Property(PropertyRef),
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
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

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
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

impl EventSegmentation {
    pub fn time_columns(&self, cur_time: DateTime<Utc>) -> Vec<String> {
        let (from, to) = self.time.range(cur_time);

        time_columns(from, to, &self.interval_unit)
    }
}
