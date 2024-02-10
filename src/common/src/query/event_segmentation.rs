use chrono::DateTime;
use chrono::Utc;
use datafusion_common::ScalarValue;
use serde::Deserialize;
use serde::Serialize;

use crate::query::time_columns;
use crate::query::AggregateFunction;
use crate::query::EventFilter;
use crate::query::EventRef;
use crate::query::PartitionedAggregateFunction;
use crate::query::PropValueOperation;
use crate::query::PropertyRef;
use crate::query::QueryTime;
use crate::query::TimeIntervalUnit;
use crate::scalar::ScalarValueRef;

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
    Each {
        n: i64,
        unit: TimeIntervalUnit,
    },
}

impl SegmentTime {
    pub fn try_window(&self) -> Option<i64> {
        match self {
            SegmentTime::Each { n, unit } => Some(unit.duration(*n).num_seconds()),
            _ => None,
        }
    }
}
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum ChartType {
    Line,
    Column,
    Pie,
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

impl Query {
    pub fn name(&self) -> String {
        match self {
            Query::CountEvents => "Count".to_string(),
            Query::CountUniqueGroups => "Count unique".to_string(),
            Query::DailyActiveGroups => "Daily active".to_string(),
            Query::WeeklyActiveGroups => "Weekly active".to_string(),
            Query::MonthlyActiveGroups => "Monthly active".to_string(),
            Query::CountPerGroup { aggregate } => match aggregate {
                AggregateFunction::Count => "Count".to_string(),
                AggregateFunction::Sum => "Sum".to_string(),
                AggregateFunction::Min => "Min".to_string(),
                AggregateFunction::Max => "Max".to_string(),
                AggregateFunction::Avg => "Avg".to_string(),
                AggregateFunction::Median => "Median".to_string(),
                AggregateFunction::ApproxDistinct => "Approx distinct".to_string(),
                AggregateFunction::ArrayAgg => "Array agg".to_string(),
                AggregateFunction::Variance => "Variance".to_string(),
                AggregateFunction::VariancePop => "Variance pop".to_string(),
                AggregateFunction::Stddev => "Std dev".to_string(),
                AggregateFunction::StddevPop => "Std dev pop".to_string(),
                AggregateFunction::Covariance => "Covariance".to_string(),
                AggregateFunction::CovariancePop => "Covariance pop".to_string(),
                AggregateFunction::Correlation => "Correlation".to_string(),
                AggregateFunction::ApproxPercentileCont => "Approx percentile cont".to_string(),
                AggregateFunction::ApproxPercentileContWithWeight => {
                    "Approx percentile cont with weight".to_string()
                }
                AggregateFunction::ApproxMedian => "Approx median".to_string(),
                AggregateFunction::Grouping => "Grouping".to_string(),
            },
            Query::AggregatePropertyPerGroup {
                property,
                aggregate_per_group,
                aggregate,
            } => {
                format!(
                    "{}({}({}))",
                    aggregate,
                    aggregate_per_group,
                    property.name()
                )
            }
            Query::AggregateProperty {
                property,
                aggregate,
            } => {
                format!("{}({})", aggregate, property.name())
            }
            Query::QueryFormula { formula } => formula.to_owned(),
        }
    }
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

#[serde_with::serde_as]
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum DidEventAggregate {
    Count {
        operation: PropValueOperation,
        value: i64,
        time: SegmentTime,
    },
    RelativeCount {
        event: EventRef,
        operation: PropValueOperation,
        filters: Option<Vec<EventFilter>>,
        time: SegmentTime,
    },
    AggregateProperty {
        property: PropertyRef,
        aggregate: QueryAggregate,
        operation: PropValueOperation,
        #[serde_as(as = "Option<ScalarValueRef>")]
        value: Option<ScalarValue>,
        time: SegmentTime,
    },
    HistoricalCount {
        operation: PropValueOperation,
        value: u64,
        time: SegmentTime,
    },
}

#[serde_with::serde_as]
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum SegmentCondition {
    HasPropertyValue {
        property_name: String,
        operation: PropValueOperation,
        #[serde_as(as = "Option<Vec<ScalarValueRef>>")]
        value: Option<Vec<ScalarValue>>,
    },
    HadPropertyValue {
        property_name: String,
        operation: PropValueOperation,
        #[serde_as(as = "Option<Vec<ScalarValueRef>>")]
        value: Option<Vec<ScalarValue>>,
        time: SegmentTime,
    },
    DidEvent {
        event: EventRef,
        filters: Option<Vec<EventFilter>>,
        aggregate: DidEventAggregate,
    },
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Segment {
    pub name: String,
    pub conditions: Vec<Vec<SegmentCondition>>, // Or<And<SegmentCondition>>
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
    pub segments: Option<Vec<Segment>>,
}

impl EventSegmentation {
    pub fn time_columns(&self, cur_time: DateTime<Utc>) -> Vec<String> {
        let (from, to) = self.time.range(cur_time);

        time_columns(from, to, &self.interval_unit)
    }
}
