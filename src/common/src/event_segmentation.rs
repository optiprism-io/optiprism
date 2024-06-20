use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use datafusion_common::ScalarValue;
use serde::Deserialize;
use serde::Serialize;

use crate::query::time_columns;
use crate::query::AggregateFunction;
use crate::query::Breakdown;
use crate::query::EventRef;
use crate::query::PartitionedAggregateFunction;
use crate::query::PropValueFilter;
use crate::query::PropValueOperation;
use crate::query::PropertyRef;
use crate::query::QueryTime;
use crate::query::Segment;
use crate::query::TimeIntervalUnit;
use crate::scalar::ScalarValueRef;

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
    pub fn initial_name(&self) -> &str {
        match self {
            Query::CountEvents => "count",
            Query::CountUniqueGroups | Query::DailyActiveGroups | Query::CountPerGroup { .. } => {
                "partitioned_count"
            }
            Query::WeeklyActiveGroups => unimplemented!(),
            Query::MonthlyActiveGroups => unimplemented!(),
            Query::AggregatePropertyPerGroup { .. } => "partitioned_agg",
            Query::AggregateProperty { .. } => "agg",
            Query::QueryFormula { .. } => unimplemented!(),
        }
    }
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
pub struct Event {
    pub event: EventRef,
    pub filters: Option<Vec<PropValueFilter>>,
    pub breakdowns: Option<Vec<Breakdown>>,
    pub queries: Vec<NamedQuery>,
}

impl Event {
    pub fn new(
        event: EventRef,
        filters: Option<Vec<PropValueFilter>>,
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
pub struct EventSegmentationRequest {
    pub time: QueryTime,
    pub group_id: usize,
    pub interval_unit: TimeIntervalUnit,
    pub chart_type: ChartType,
    pub analysis: Analysis,
    pub compare: Option<Compare>,
    pub events: Vec<Event>,
    pub filters: Option<Vec<PropValueFilter>>,
    pub breakdowns: Option<Vec<Breakdown>>,
    pub segments: Option<Vec<Segment>>,
}

impl EventSegmentationRequest {
    pub fn time_columns(&self, cur_time: DateTime<Utc>) -> Vec<String> {
        let (from, to) = self.time.range(cur_time);
        time_columns(from, to, &self.interval_unit)
    }
}
