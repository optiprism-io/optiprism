use chrono::DateTime;
use chrono::Utc;
use common::query::event_segmentation::NamedQuery;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

use crate::error::Result;
use crate::json_value_to_scalar;
use crate::queries::AggregateFunction;
use crate::queries::PartitionedAggregateFunction;
use crate::queries::QueryTime;
use crate::queries::TimeIntervalUnit;
use crate::scalar_to_json_value;
use crate::EventFilter;
use crate::EventGroupedFilters;
use crate::EventRef;
use crate::PlatformError;
use crate::PropValueOperation;
use crate::PropertyRef;

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum SegmentTime {
    Between {
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    },
    From(DateTime<Utc>),
    Last {
        last: i64,
        unit: TimeIntervalUnit,
    },
    AfterFirstUse {
        within: i64,
        unit: TimeIntervalUnit,
    },
    WindowEach {
        unit: TimeIntervalUnit,
        n: i64,
    },
}

impl TryInto<common::query::event_segmentation::SegmentTime> for SegmentTime {
    type Error = PlatformError;

    fn try_into(
        self,
    ) -> std::result::Result<common::query::event_segmentation::SegmentTime, Self::Error> {
        Ok(match self {
            SegmentTime::Between { from, to } => {
                common::query::event_segmentation::SegmentTime::Between { from, to }
            }
            SegmentTime::From(v) => common::query::event_segmentation::SegmentTime::From(v),
            SegmentTime::Last { last: n, unit } => {
                common::query::event_segmentation::SegmentTime::Last {
                    n,
                    unit: unit.try_into()?,
                }
            }
            SegmentTime::AfterFirstUse { within, unit } => {
                common::query::event_segmentation::SegmentTime::AfterFirstUse {
                    within,
                    unit: unit.try_into()?,
                }
            }
            SegmentTime::WindowEach { unit, n } => {
                common::query::event_segmentation::SegmentTime::Each {
                    n,
                    unit: unit.try_into()?,
                }
            }
        })
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ChartType {
    Line,
    Column,
    Pie,
}

impl TryInto<common::query::event_segmentation::ChartType> for ChartType {
    type Error = PlatformError;

    fn try_into(
        self,
    ) -> std::result::Result<common::query::event_segmentation::ChartType, Self::Error> {
        Ok(match self {
            ChartType::Line => common::query::event_segmentation::ChartType::Line,
            ChartType::Column => common::query::event_segmentation::ChartType::Column,
            ChartType::Pie => common::query::event_segmentation::ChartType::Pie,
        })
    }
}

impl TryInto<ChartType> for common::query::event_segmentation::ChartType {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<ChartType, Self::Error> {
        Ok(match self {
            common::query::event_segmentation::ChartType::Line => ChartType::Line,
            common::query::event_segmentation::ChartType::Column => ChartType::Column,
            common::query::event_segmentation::ChartType::Pie => ChartType::Pie,
        })
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum Analysis {
    Linear,
    RollingAverage {
        window: usize,
        unit: TimeIntervalUnit,
    },
    Logarithmic,
    Cumulative,
}

impl TryInto<common::query::event_segmentation::Analysis> for Analysis {
    type Error = PlatformError;

    fn try_into(
        self,
    ) -> std::result::Result<common::query::event_segmentation::Analysis, Self::Error> {
        Ok(match self {
            Analysis::Linear => common::query::event_segmentation::Analysis::Linear,
            Analysis::RollingAverage { window, unit } => {
                common::query::event_segmentation::Analysis::RollingAverage {
                    window,
                    unit: unit.try_into()?,
                }
            }
            Analysis::Logarithmic => common::query::event_segmentation::Analysis::Logarithmic,
            Analysis::Cumulative => common::query::event_segmentation::Analysis::Cumulative,
        })
    }
}

impl TryInto<Analysis> for common::query::event_segmentation::Analysis {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<Analysis, Self::Error> {
        Ok(match self {
            common::query::event_segmentation::Analysis::Linear => Analysis::Linear,
            common::query::event_segmentation::Analysis::RollingAverage { window, unit } => {
                Analysis::RollingAverage {
                    window,
                    unit: unit.try_into()?,
                }
            }
            common::query::event_segmentation::Analysis::Logarithmic => Analysis::Logarithmic,
            common::query::event_segmentation::Analysis::Cumulative => Analysis::Cumulative,
        })
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Compare {
    pub offset: usize,
    pub unit: TimeIntervalUnit,
}

impl TryInto<common::query::event_segmentation::Compare> for Compare {
    type Error = PlatformError;

    fn try_into(
        self,
    ) -> std::result::Result<common::query::event_segmentation::Compare, Self::Error> {
        Ok(common::query::event_segmentation::Compare {
            offset: self.offset,
            unit: self.unit.try_into()?,
        })
    }
}

impl TryInto<Compare> for common::query::event_segmentation::Compare {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<Compare, Self::Error> {
        Ok(Compare {
            offset: self.offset,
            unit: self.unit.try_into()?,
        })
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
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

impl TryInto<common::query::event_segmentation::QueryAggregate> for QueryAggregate {
    type Error = PlatformError;

    fn try_into(
        self,
    ) -> std::result::Result<common::query::event_segmentation::QueryAggregate, Self::Error> {
        Ok(match self {
            QueryAggregate::Min => common::query::event_segmentation::QueryAggregate::Min,
            QueryAggregate::Max => common::query::event_segmentation::QueryAggregate::Max,
            QueryAggregate::Sum => common::query::event_segmentation::QueryAggregate::Sum,
            QueryAggregate::Avg => common::query::event_segmentation::QueryAggregate::Avg,
            QueryAggregate::Median => common::query::event_segmentation::QueryAggregate::Median,
            QueryAggregate::DistinctCount => {
                common::query::event_segmentation::QueryAggregate::DistinctCount
            }
            QueryAggregate::Percentile25th => {
                common::query::event_segmentation::QueryAggregate::Percentile25th
            }
            QueryAggregate::Percentile75th => {
                common::query::event_segmentation::QueryAggregate::Percentile75th
            }
            QueryAggregate::Percentile90th => {
                common::query::event_segmentation::QueryAggregate::Percentile90th
            }
            QueryAggregate::Percentile99th => {
                common::query::event_segmentation::QueryAggregate::Percentile99th
            }
        })
    }
}

impl TryInto<QueryAggregate> for common::query::event_segmentation::QueryAggregate {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<QueryAggregate, Self::Error> {
        Ok(match self {
            common::query::event_segmentation::QueryAggregate::Min => QueryAggregate::Min,
            common::query::event_segmentation::QueryAggregate::Max => QueryAggregate::Max,
            common::query::event_segmentation::QueryAggregate::Sum => QueryAggregate::Sum,
            common::query::event_segmentation::QueryAggregate::Avg => QueryAggregate::Avg,
            common::query::event_segmentation::QueryAggregate::Median => QueryAggregate::Median,
            common::query::event_segmentation::QueryAggregate::DistinctCount => {
                QueryAggregate::DistinctCount
            }
            common::query::event_segmentation::QueryAggregate::Percentile25th => {
                QueryAggregate::Percentile25th
            }
            common::query::event_segmentation::QueryAggregate::Percentile75th => {
                QueryAggregate::Percentile75th
            }
            common::query::event_segmentation::QueryAggregate::Percentile90th => {
                QueryAggregate::Percentile90th
            }
            common::query::event_segmentation::QueryAggregate::Percentile99th => {
                QueryAggregate::Percentile99th
            }
        })
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum QueryAggregatePerGroup {
    Min,
    Max,
    Sum,
    Avg,
    Median,
    DistinctCount,
}

impl TryInto<common::query::event_segmentation::QueryAggregatePerGroup> for QueryAggregatePerGroup {
    type Error = PlatformError;

    fn try_into(
        self,
    ) -> std::result::Result<common::query::event_segmentation::QueryAggregatePerGroup, Self::Error>
    {
        Ok(match self {
            QueryAggregatePerGroup::Min => {
                common::query::event_segmentation::QueryAggregatePerGroup::Min
            }
            QueryAggregatePerGroup::Max => {
                common::query::event_segmentation::QueryAggregatePerGroup::Max
            }
            QueryAggregatePerGroup::Sum => {
                common::query::event_segmentation::QueryAggregatePerGroup::Sum
            }
            QueryAggregatePerGroup::Avg => {
                common::query::event_segmentation::QueryAggregatePerGroup::Avg
            }
            QueryAggregatePerGroup::Median => {
                common::query::event_segmentation::QueryAggregatePerGroup::Median
            }
            QueryAggregatePerGroup::DistinctCount => {
                common::query::event_segmentation::QueryAggregatePerGroup::DistinctCount
            }
        })
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum QueryPerGroup {
    CountEvents,
}

impl TryInto<common::query::event_segmentation::QueryPerGroup> for QueryPerGroup {
    type Error = PlatformError;

    fn try_into(
        self,
    ) -> std::result::Result<common::query::event_segmentation::QueryPerGroup, Self::Error> {
        Ok(match self {
            QueryPerGroup::CountEvents => {
                common::query::event_segmentation::QueryPerGroup::CountEvents
            }
        })
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum Query {
    CountEvents,
    CountUniqueGroups,
    DailyActiveGroups,
    WeeklyActiveGroups,
    MonthlyActiveGroups,
    CountPerGroup {
        aggregate: AggregateFunction,
    },
    #[serde(rename_all = "camelCase")]
    AggregatePropertyPerGroup {
        #[serde(flatten)]
        property: PropertyRef,
        aggregate_per_group: PartitionedAggregateFunction,
        aggregate: AggregateFunction,
    },
    #[serde(rename_all = "camelCase")]
    AggregateProperty {
        #[serde(flatten)]
        property: PropertyRef,
        aggregate: AggregateFunction,
    },
    Formula {
        formula: String,
    },
}

impl TryInto<common::query::event_segmentation::Query> for &Query {
    type Error = PlatformError;

    fn try_into(
        self,
    ) -> std::result::Result<common::query::event_segmentation::Query, Self::Error> {
        Ok(match self {
            Query::CountEvents => common::query::event_segmentation::Query::CountEvents,
            Query::CountUniqueGroups => common::query::event_segmentation::Query::CountUniqueGroups,
            Query::DailyActiveGroups => common::query::event_segmentation::Query::DailyActiveGroups,
            Query::WeeklyActiveGroups => {
                common::query::event_segmentation::Query::WeeklyActiveGroups
            }
            Query::MonthlyActiveGroups => {
                common::query::event_segmentation::Query::MonthlyActiveGroups
            }
            Query::CountPerGroup { aggregate } => {
                common::query::event_segmentation::Query::CountPerGroup {
                    aggregate: aggregate.try_into()?,
                }
            }
            Query::AggregatePropertyPerGroup {
                property,
                aggregate_per_group,
                aggregate,
            } => common::query::event_segmentation::Query::AggregatePropertyPerGroup {
                property: property.to_owned().try_into()?,
                aggregate_per_group: aggregate_per_group.try_into()?,
                aggregate: aggregate.try_into()?,
            },
            Query::AggregateProperty {
                property,
                aggregate,
            } => common::query::event_segmentation::Query::AggregateProperty {
                property: property.to_owned().try_into()?,
                aggregate: aggregate.try_into()?,
            },
            Query::Formula { formula } => common::query::event_segmentation::Query::QueryFormula {
                formula: formula.clone(),
            },
        })
    }
}

impl TryInto<Query> for common::query::event_segmentation::Query {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<Query, Self::Error> {
        Ok(match self {
            common::query::event_segmentation::Query::CountEvents => Query::CountEvents,
            common::query::event_segmentation::Query::CountUniqueGroups => Query::CountUniqueGroups,
            common::query::event_segmentation::Query::DailyActiveGroups => Query::DailyActiveGroups,
            common::query::event_segmentation::Query::WeeklyActiveGroups => {
                Query::WeeklyActiveGroups
            }
            common::query::event_segmentation::Query::MonthlyActiveGroups => {
                Query::MonthlyActiveGroups
            }
            common::query::event_segmentation::Query::CountPerGroup { aggregate } => {
                Query::CountPerGroup {
                    aggregate: aggregate.try_into()?,
                }
            }
            common::query::event_segmentation::Query::AggregatePropertyPerGroup {
                property,
                aggregate_per_group,
                aggregate,
            } => Query::AggregatePropertyPerGroup {
                property: property.try_into()?,
                aggregate_per_group: aggregate_per_group.try_into()?,
                aggregate: aggregate.try_into()?,
            },
            common::query::event_segmentation::Query::AggregateProperty {
                property,
                aggregate,
            } => Query::AggregateProperty {
                property: property.try_into()?,
                aggregate: aggregate.try_into()?,
            },
            common::query::event_segmentation::Query::QueryFormula { formula } => {
                Query::Formula { formula }
            }
        })
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum Breakdown {
    Property {
        #[serde(flatten)]
        property: PropertyRef,
    },
}

impl TryInto<common::query::event_segmentation::Breakdown> for &Breakdown {
    type Error = PlatformError;

    fn try_into(
        self,
    ) -> std::result::Result<common::query::event_segmentation::Breakdown, Self::Error> {
        Ok(match self {
            Breakdown::Property { property } => {
                common::query::event_segmentation::Breakdown::Property(
                    property.to_owned().try_into()?,
                )
            }
        })
    }
}

impl TryInto<Breakdown> for &common::query::event_segmentation::Breakdown {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<Breakdown, Self::Error> {
        Ok(match self {
            common::query::event_segmentation::Breakdown::Property(property) => {
                Breakdown::Property {
                    property: property.to_owned().try_into()?,
                }
            }
        })
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub enum EventType {
    Regular,
    Custom,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Event {
    #[serde(flatten)]
    pub event: EventRef,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filters: Option<Vec<EventFilter>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub breakdowns: Option<Vec<Breakdown>>,
    pub queries: Vec<Query>,
}

impl TryInto<common::query::event_segmentation::Event> for &Event {
    type Error = PlatformError;

    fn try_into(
        self,
    ) -> std::result::Result<common::query::event_segmentation::Event, Self::Error> {
        Ok(common::query::event_segmentation::Event {
            event: self.event.to_owned().into(),
            filters: self
                .filters
                .as_ref()
                .map_or_else(
                    || None,
                    |v| {
                        if v.is_empty() {
                            None
                        } else {
                            Some(v.iter().map(|v| v.try_into()).collect::<Result<_>>())
                        }
                    },
                )
                .transpose()?,
            breakdowns: self
                .breakdowns
                .as_ref()
                .map_or_else(
                    || None,
                    |v| {
                        if v.is_empty() {
                            None
                        } else {
                            Some(v.iter().map(|v| v.try_into()).collect::<Result<_>>())
                        }
                    },
                )
                .transpose()?,
            queries: self
                .queries
                .iter()
                .map(|v| v.try_into())
                .collect::<std::result::Result<Vec<common::query::event_segmentation::Query>, _>>()?
                .iter()
                .enumerate()
                .map(|(idx, v)| NamedQuery::new(v.clone(), Some(self.event.name(idx))))
                .collect(),
        })
    }
}

impl TryInto<Event> for &common::query::event_segmentation::Event {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<Event, Self::Error> {
        Ok(Event {
            event: self.event.to_owned().try_into()?,
            filters: self
                .filters
                .as_ref()
                .map_or_else(
                    || None,
                    |v| {
                        if v.is_empty() {
                            None
                        } else {
                            Some(v.iter().map(|v| v.try_into()).collect::<Result<_>>())
                        }
                    },
                )
                .transpose()?,
            breakdowns: self
                .breakdowns
                .as_ref()
                .map_or_else(
                    || None,
                    |v| {
                        if v.is_empty() {
                            None
                        } else {
                            Some(v.iter().map(|v| v.try_into()).collect::<Result<_>>())
                        }
                    },
                )
                .transpose()?,
            queries: self
                .queries
                .iter()
                .map(|v| v.agg.clone().try_into())
                .collect::<std::result::Result<Vec<Query>, _>>()?,
        })
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum DidEventAggregate {
    Count {
        operation: PropValueOperation,
        value: i64,
        time: SegmentTime,
    },
    RelativeCount {
        #[serde(flatten)]
        event: EventRef,
        operation: PropValueOperation,
        filters: Option<Vec<EventFilter>>,
        time: SegmentTime,
    },
    AggregateProperty {
        #[serde(flatten)]
        property: PropertyRef,
        aggregate: QueryAggregate,
        operation: PropValueOperation,
        value: Option<Value>,
        time: SegmentTime,
    },
    HistoricalCount {
        operation: PropValueOperation,
        value: u64,
        time: SegmentTime,
    },
}

impl TryInto<common::query::event_segmentation::DidEventAggregate> for DidEventAggregate {
    type Error = PlatformError;

    fn try_into(
        self,
    ) -> std::result::Result<common::query::event_segmentation::DidEventAggregate, Self::Error>
    {
        Ok(match self {
            DidEventAggregate::Count {
                operation,
                value,
                time,
            } => common::query::event_segmentation::DidEventAggregate::Count {
                operation: operation.try_into()?,
                value,
                time: time.try_into()?,
            },
            DidEventAggregate::RelativeCount {
                event,
                operation,
                filters,
                time,
            } => common::query::event_segmentation::DidEventAggregate::RelativeCount {
                event: event.into(),
                operation: operation.try_into()?,
                filters: filters
                    .map_or_else(
                        || None,
                        |v| {
                            if v.is_empty() {
                                None
                            } else {
                                Some(v.iter().map(|v| v.try_into()).collect::<Result<_>>())
                            }
                        },
                    )
                    .transpose()?,
                time: time.try_into()?,
            },
            DidEventAggregate::AggregateProperty {
                property,
                aggregate,
                operation,
                value,
                time,
            } => common::query::event_segmentation::DidEventAggregate::AggregateProperty {
                property: property.try_into()?,
                aggregate: aggregate.try_into()?,
                operation: operation.try_into()?,
                value: value.map(|v| json_value_to_scalar(&v)).transpose()?,
                time: time.try_into()?,
            },
            DidEventAggregate::HistoricalCount {
                operation,
                value,
                time,
            } => common::query::event_segmentation::DidEventAggregate::HistoricalCount {
                operation: operation.try_into()?,
                value,
                time: time.try_into()?,
            },
        })
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum SegmentCondition {
    #[serde(rename_all = "camelCase")]
    HasPropertyValue {
        property_name: String,
        operation: PropValueOperation,
        #[serde(skip_serializing_if = "Option::is_none")]
        value: Option<Vec<Value>>,
    },
    #[serde(rename_all = "camelCase")]
    HadPropertyValue {
        property_name: String,
        operation: PropValueOperation,
        #[serde(skip_serializing_if = "Option::is_none")]
        value: Option<Vec<Value>>,
        time: SegmentTime,
    },
    #[serde(rename_all = "camelCase")]
    DidEvent {
        #[serde(flatten)]
        event: EventRef,
        filters: Option<Vec<EventFilter>>,
        aggregate: DidEventAggregate,
    },
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Segment {
    name: String,
    conditions: Vec<Vec<SegmentCondition>>,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct EventSegmentation {
    pub time: QueryTime,
    pub group: String,
    pub interval_unit: TimeIntervalUnit,
    pub chart_type: ChartType,
    pub analysis: Analysis,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compare: Option<Compare>,
    pub events: Vec<Event>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filters: Option<EventGroupedFilters>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub breakdowns: Option<Vec<Breakdown>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub segments: Option<Vec<Segment>>,
}

impl TryInto<common::query::event_segmentation::SegmentCondition> for SegmentCondition {
    type Error = PlatformError;

    fn try_into(
        self,
    ) -> std::result::Result<common::query::event_segmentation::SegmentCondition, Self::Error> {
        Ok(match self {
            SegmentCondition::HasPropertyValue {
                property_name,
                operation,
                value,
            } => common::query::event_segmentation::SegmentCondition::HasPropertyValue {
                property_name,
                operation: operation.try_into()?,
                value: match value {
                    Some(v) if !v.is_empty() => {
                        Some(v.iter().map(json_value_to_scalar).collect::<Result<_>>()?)
                    }
                    _ => None,
                },
                // value
                // .map(|v| {
                // if v.is_empty() {
                // None
                // } else {
                // v.iter()
                // .map(|v| json_value_to_scalar(v))
                // .collect::<Result<_>>()
                // }
                // })
                // .transpose()?,
            },
            SegmentCondition::HadPropertyValue {
                property_name,
                operation,
                value,
                time,
            } => common::query::event_segmentation::SegmentCondition::HadPropertyValue {
                property_name,
                operation: operation.try_into()?,
                value: match value {
                    Some(v) if !v.is_empty() => {
                        Some(v.iter().map(json_value_to_scalar).collect::<Result<_>>()?)
                    }
                    _ => None,
                },
                time: time.try_into()?,
            },
            SegmentCondition::DidEvent {
                event,
                filters,
                aggregate,
            } => common::query::event_segmentation::SegmentCondition::DidEvent {
                event: event.into(),
                filters: filters
                    .map_or_else(
                        || None,
                        |v| {
                            if v.is_empty() {
                                None
                            } else {
                                Some(v.iter().map(|v| v.try_into()).collect::<Result<_>>())
                            }
                        },
                    )
                    .transpose()?,
                aggregate: aggregate.try_into()?,
            },
        })
    }
}

impl TryInto<SegmentTime> for common::query::event_segmentation::SegmentTime {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<SegmentTime, Self::Error> {
        Ok(match self {
            common::query::event_segmentation::SegmentTime::Between { from, to } => {
                SegmentTime::Between { from, to }
            }
            common::query::event_segmentation::SegmentTime::From(from) => SegmentTime::From(from),
            common::query::event_segmentation::SegmentTime::Last { n, unit } => SegmentTime::Last {
                last: n,
                unit: unit.try_into()?,
            },
            common::query::event_segmentation::SegmentTime::AfterFirstUse { within, unit } => {
                SegmentTime::AfterFirstUse {
                    within,
                    unit: unit.try_into()?,
                }
            }
            common::query::event_segmentation::SegmentTime::Each { n, unit } => {
                SegmentTime::WindowEach {
                    unit: unit.try_into()?,
                    n,
                }
            }
        })
    }
}

impl TryInto<DidEventAggregate> for common::query::event_segmentation::DidEventAggregate {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<DidEventAggregate, Self::Error> {
        Ok(match self {
            common::query::event_segmentation::DidEventAggregate::Count {
                operation,
                value,
                time,
            } => DidEventAggregate::Count {
                operation: operation.try_into()?,
                value,
                time: time.try_into()?,
            },
            common::query::event_segmentation::DidEventAggregate::RelativeCount {
                event,
                operation,
                filters,
                time,
            } => DidEventAggregate::RelativeCount {
                event: event.try_into()?,
                operation: operation.try_into()?,
                filters: filters
                    .map_or_else(
                        || None,
                        |v| {
                            if v.is_empty() {
                                None
                            } else {
                                Some(v.iter().map(|v| v.try_into()).collect::<Result<_>>())
                            }
                        },
                    )
                    .transpose()?,
                time: time.try_into()?,
            },
            common::query::event_segmentation::DidEventAggregate::AggregateProperty {
                property,
                aggregate,
                operation,
                value,
                time,
            } => DidEventAggregate::AggregateProperty {
                property: property.try_into()?,
                aggregate: aggregate.try_into()?,
                operation: operation.try_into()?,
                value: value.map(|v| scalar_to_json_value(&v)).transpose()?,
                time: time.try_into()?,
            },
            common::query::event_segmentation::DidEventAggregate::HistoricalCount {
                operation,
                value,
                time,
            } => DidEventAggregate::HistoricalCount {
                operation: operation.try_into()?,
                value,
                time: time.try_into()?,
            },
        })
    }
}

impl TryInto<SegmentCondition> for common::query::event_segmentation::SegmentCondition {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<SegmentCondition, Self::Error> {
        Ok(match self {
            common::query::event_segmentation::SegmentCondition::HasPropertyValue {
                property_name,
                operation,
                value,
            } => SegmentCondition::HasPropertyValue {
                property_name,
                operation: operation.try_into()?,
                value: value
                    .map_or_else(
                        || None,
                        |v| {
                            if v.is_empty() {
                                None
                            } else {
                                Some(v.iter().map(scalar_to_json_value).collect::<Result<_>>())
                            }
                        },
                    )
                    .transpose()?,
            },
            common::query::event_segmentation::SegmentCondition::HadPropertyValue {
                property_name,
                operation,
                value,
                time,
            } => SegmentCondition::HadPropertyValue {
                property_name,
                operation: operation.try_into()?,
                value: value
                    .map_or_else(
                        || None,
                        |v| {
                            if v.is_empty() {
                                None
                            } else {
                                Some(v.iter().map(scalar_to_json_value).collect::<Result<_>>())
                            }
                        },
                    )
                    .transpose()?,

                time: time.try_into()?,
            },
            common::query::event_segmentation::SegmentCondition::DidEvent {
                event,
                filters,
                aggregate,
            } => SegmentCondition::DidEvent {
                event: event.try_into()?,
                filters: filters
                    .map_or_else(
                        || None,
                        |v| {
                            if v.is_empty() {
                                None
                            } else {
                                Some(v.iter().map(|v| v.try_into()).collect::<Result<_>>())
                            }
                        },
                    )
                    .transpose()?,
                aggregate: aggregate.try_into()?,
            },
        })
    }
}

impl TryInto<common::query::event_segmentation::Segment> for Segment {
    type Error = PlatformError;

    fn try_into(
        self,
    ) -> std::result::Result<common::query::event_segmentation::Segment, Self::Error> {
        Ok(common::query::event_segmentation::Segment {
            name: self.name.clone(),
            conditions: self
                .conditions
                .iter()
                .map(|v| {
                    v.iter()
                        .map(|v| v.to_owned().try_into())
                        .collect::<Result<Vec<_>>>()
                })
                .collect::<Result<_>>()?,
        })
    }
}

impl TryInto<Segment> for common::query::event_segmentation::Segment {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<Segment, Self::Error> {
        Ok(Segment {
            name: self.name.clone(),
            conditions: self
                .conditions
                .iter()
                .map(|v| {
                    v.iter()
                        .map(|v| v.to_owned().try_into())
                        .collect::<Result<Vec<_>>>()
                })
                .collect::<Result<_>>()?,
        })
    }
}

// impl TryInto<common::query::event_segmentation::SegmentCondition> for SegmentCondition {
// type Error = PlatformError;
//
// fn try_into(self) -> Result<common::query::event_segmentation::SegmentCondition, Self::Error> {
// match self {
// SegmentCondition::HasPropertyValue {
// property_name,
// operation,
// value,
// } => common::query::event_segmentation::SegmentCondition::HasPropertyValue {
// property_name,
// operation: operation.try_into()?,
// value: value
// .map(|v| v.iter().map(json_value_to_scalar))
// .collect::<std::result::Result<_, _>>()
// .transpose()?,
// },
// SegmentCondition::HadPropertyValue {
// property_name,
// operation,
// value,
// time,
// } => common::query::event_segmentation::SegmentCondition::HadPropertyValue {
// property_name,
// operation: operation.try_into()?,
// value: value
// .map(|v| v.iter().map(json_value_to_scalar))
// .collect::<Result<Vec<_>, _>>()
// .transpose()?,
// time: time.try_into()?,
// },
// SegmentCondition::DidEvent {
// event,
// filters,
// aggregate,
// } => common::query::event_segmentation::SegmentCondition::DidEvent {
// event: event.try_into()?,
// filters: filters
// .map(|v| v.iter().map(|v| v.try_into()))
// .collect::<std::result::Result<Vec<_>, _>>()
// .transpose()?,
// aggregate: aggregate.try_into()?,
// },
// }
// }
// }
//
// impl TryInto<SegmentCondition> for common::query::event_segmentation::SegmentCondition {
// type Error = PlatformError;
//
// fn try_into(self) -> Result<SegmentCondition, Self::Error> {
// match self {
// common::query::event_segmentation::SegmentCondition::HasPropertyValue {
// property_name,
// operation,
// value,
// } => SegmentCondition::HasPropertyValue {
// property_name,
// operation: operation.try_into()?,
// value: value
// .map(|v| v.iter().map(|v| json_value_to_scalar(v)))
// .collect::<std::result::Result<_, _>>()
// .transpose()?,
// },
// common::query::event_segmentation::SegmentCondition::HadPropertyValue {
// property_name,
// operation,
// value,
// time,
// } => SegmentCondition::HadPropertyValue {
// property_name,
// operation: operation.try_into()?,
// value: value
// .map(|v| v.iter().map(|v| json_value_to_scalar(v)))
// .collect::<std::result::Result<_, _>>()
// .transpose()?,
// time: time.try_into()?,
// },
// SegmentCondition::DidEvent {
// event,
// filters,
// aggregate,
// } => common::query::event_segmentation::SegmentCondition::DidEvent {
// event: event.try_into()?,
// filters: filters
// .map(|v| v.iter().map(|v| scalar_to_json_value(v)))
// .collect::<std::result::Result<_, _>>()
// .transpose()?,
// aggregate: aggregate.try_into()?,
// },
// }
// }
// }

impl TryInto<common::query::event_segmentation::EventSegmentation> for EventSegmentation {
    type Error = PlatformError;

    fn try_into(
        self,
    ) -> std::result::Result<common::query::event_segmentation::EventSegmentation, Self::Error>
    {
        Ok(common::query::event_segmentation::EventSegmentation {
            time: self.time.try_into()?,
            group: self.group,
            interval_unit: self.interval_unit.try_into()?,
            chart_type: self.chart_type.try_into()?,
            analysis: self.analysis.try_into()?,
            compare: self.compare.map(|v| v.try_into()).transpose()?,
            events: self
                .events
                .iter()
                .map(|v| v.try_into())
                .collect::<std::result::Result<_, _>>()?,
            filters: None,
            // filters: self.filters.map(|v| v.try_into()).transpose()?,
            breakdowns: self
                .breakdowns
                .map_or_else(
                    || None,
                    |v| {
                        if v.is_empty() {
                            None
                        } else {
                            Some(v.iter().map(|v| v.try_into()).collect::<Result<_>>())
                        }
                    },
                )
                .transpose()?,
            segments: self
                .segments
                .map_or_else(
                    || None,
                    |v| {
                        if v.is_empty() {
                            None
                        } else {
                            Some(
                                v.iter()
                                    .map(|v| v.to_owned().try_into())
                                    .collect::<Result<_>>(),
                            )
                        }
                    },
                )
                .transpose()?,
        })
    }
}

impl TryInto<EventSegmentation> for common::query::event_segmentation::EventSegmentation {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<EventSegmentation, Self::Error> {
        Ok(EventSegmentation {
            time: self.time.try_into()?,
            group: self.group,
            interval_unit: self.interval_unit.try_into()?,
            chart_type: self.chart_type.try_into()?,
            analysis: self.analysis.try_into()?,
            compare: self.compare.map(|v| v.try_into()).transpose()?,
            events: self
                .events
                .iter()
                .map(|v| v.try_into())
                .collect::<std::result::Result<_, _>>()?,
            filters: None,
            // filters: self
            //     .filters
            //     .map(|v| {
            //         v.iter()
            //             .map(|v| v.try_into())
            //             .collect::<std::result::Result<_, _>>()
            //     })
            //     .transpose()?,
            breakdowns: self
                .breakdowns
                .map_or_else(
                    || None,
                    |v| {
                        if v.is_empty() {
                            None
                        } else {
                            Some(v.iter().map(|v| v.try_into()).collect::<Result<_>>())
                        }
                    },
                )
                .transpose()?,
            segments: self
                .segments
                .map(|v| {
                    v.iter()
                        .map(|v| v.to_owned().try_into())
                        .collect::<std::result::Result<_, _>>()
                })
                .transpose()?,
        })
    }
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;
    use chrono::Utc;
    use query::event_fields;
    use serde_json::json;

    use crate::error::Result;
    use crate::queries::event_segmentation::AggregateFunction;
    use crate::queries::event_segmentation::Analysis;
    use crate::queries::event_segmentation::Breakdown;
    use crate::queries::event_segmentation::ChartType;
    use crate::queries::event_segmentation::Compare;
    use crate::queries::event_segmentation::Event;
    use crate::queries::event_segmentation::EventFilter;
    use crate::queries::event_segmentation::EventSegmentation;
    use crate::queries::event_segmentation::PartitionedAggregateFunction;
    use crate::queries::event_segmentation::Query;
    use crate::queries::event_segmentation::QueryTime;
    use crate::queries::event_segmentation::TimeIntervalUnit;
    use crate::EventGroupedFilters;
    use crate::EventRef;
    use crate::PropValueOperation;
    use crate::PropertyRef;

    #[test]
    fn test_serialize() -> Result<()> {
        let from = DateTime::parse_from_rfc3339("2021-09-08T13:42:00.000000+00:00")
            .unwrap()
            .with_timezone(&Utc);
        let to = DateTime::parse_from_rfc3339("2021-09-08T13:48:00.000000+00:00")
            .unwrap()
            .with_timezone(&Utc);
        let es = EventSegmentation {
            time: QueryTime::Between { from, to },
            group: event_fields::USER_ID.to_string(),
            interval_unit: TimeIntervalUnit::Minute,
            chart_type: ChartType::Line,
            analysis: Analysis::Linear,
            compare: Some(Compare {
                offset: 1,
                unit: TimeIntervalUnit::Second,
            }),
            events: vec![Event {
                event: EventRef::Regular {
                    event_name: "e1".to_string(),
                },
                filters: Some(vec![
                    EventFilter::Property {
                        property: PropertyRef::User {
                            property_name: "p1".to_string(),
                        },
                        operation: PropValueOperation::Eq,
                        value: Some(vec![json!(true)]),
                    },
                    EventFilter::Property {
                        property: PropertyRef::Event {
                            property_name: "p2".to_string(),
                        },
                        operation: PropValueOperation::Eq,
                        value: Some(vec![json!(true)]),
                    },
                    EventFilter::Property {
                        property: PropertyRef::Event {
                            property_name: "p3".to_string(),
                        },
                        operation: PropValueOperation::Empty,
                        value: None,
                    },
                    EventFilter::Property {
                        property: PropertyRef::Event {
                            property_name: "p4".to_string(),
                        },
                        operation: PropValueOperation::Eq,
                        value: Some(vec![json!("s")]),
                    },
                ]),
                breakdowns: Some(vec![Breakdown::Property {
                    property: PropertyRef::User {
                        property_name: "Device".to_string(),
                    },
                }]),
                queries: vec![
                    Query::CountEvents,
                    Query::CountUniqueGroups,
                    Query::CountPerGroup {
                        aggregate: AggregateFunction::Avg,
                    },
                    Query::AggregatePropertyPerGroup {
                        property: PropertyRef::Event {
                            property_name: "Revenue".to_string(),
                        },
                        aggregate_per_group: PartitionedAggregateFunction::Sum,
                        aggregate: AggregateFunction::Avg,
                    },
                    Query::AggregateProperty {
                        property: PropertyRef::Event {
                            property_name: "Revenue".to_string(),
                        },
                        aggregate: AggregateFunction::Sum,
                    },
                ],
            }],
            filters: Some(EventGroupedFilters {
                groups_condition: None,
                groups: vec![],
            }),
            // filters: Some(vec![EventFilter::Property {
            // property: PropertyRef::User {
            // property_name: "p1".to_string(),
            // },
            // operation: PropValueOperation::Eq,
            // value: Some(vec![json!(true)]),
            // }]),
            breakdowns: Some(vec![Breakdown::Property {
                property: PropertyRef::User {
                    property_name: "Device".to_string(),
                },
            }]),
            segments: None,
        };

        let _qes: common::query::event_segmentation::EventSegmentation = es.clone().try_into()?;
        let j = serde_json::to_string_pretty(&es).unwrap();
        print!("1 {}", j);

        Ok(())
    }
}
