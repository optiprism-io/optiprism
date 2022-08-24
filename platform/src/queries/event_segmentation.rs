use crate::queries::types::{
    json_value_to_scalar, AggregateFunction, EventRef, PartitionedAggregateFunction,
    PropValueOperation, PropertyRef, QueryTime, TimeUnit,
};
use crate::PlatformError;
use crate::Result;
use chrono::{DateTime, Utc};
use query::queries::event_segmentation::types as query_es_types;
use query::queries::event_segmentation::types::NamedQuery;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
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

impl TryInto<query_es_types::SegmentTime> for SegmentTime {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<query_es_types::SegmentTime, Self::Error> {
        Ok(match self {
            SegmentTime::Between { from, to } => query_es_types::SegmentTime::Between { from, to },
            SegmentTime::From(v) => query_es_types::SegmentTime::From(v),
            SegmentTime::Last { n, unit } => query_es_types::SegmentTime::Last {
                n,
                unit: unit.try_into()?,
            },
            SegmentTime::AfterFirstUse { within, unit } => {
                query_es_types::SegmentTime::AfterFirstUse {
                    within,
                    unit: unit.try_into()?,
                }
            }
            SegmentTime::WindowEach { unit } => query_es_types::SegmentTime::WindowEach {
                unit: unit.try_into()?,
            },
        })
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ChartType {
    Line,
    Bar,
}

impl TryInto<query_es_types::ChartType> for ChartType {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<query_es_types::ChartType, Self::Error> {
        Ok(match self {
            ChartType::Line => query_es_types::ChartType::Line,
            ChartType::Bar => query_es_types::ChartType::Bar,
        })
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum Analysis {
    Linear,
    RollingAverage { window: usize, unit: TimeUnit },
    WindowAverage { window: usize, unit: TimeUnit },
    Cumulative,
}

impl TryInto<query_es_types::Analysis> for Analysis {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<query_es_types::Analysis, Self::Error> {
        Ok(match self {
            Analysis::Linear => query_es_types::Analysis::Linear,
            Analysis::RollingAverage { window, unit } => query_es_types::Analysis::RollingAverage {
                window,
                unit: unit.try_into()?,
            },
            Analysis::WindowAverage { window, unit } => query_es_types::Analysis::WindowAverage {
                window,
                unit: unit.try_into()?,
            },
            Analysis::Cumulative => query_es_types::Analysis::Cumulative,
        })
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Compare {
    pub offset: usize,
    pub unit: TimeUnit,
}

/*impl TryFrom<Option<Compare>> for Option<query_es_types::Compare> {
    type Error = Error;

    fn try_from(value: Option<Compare>) -> std::result::Result<Self, Self::Error> {
        Ok(match value {
            None => None,
            Some(v) => v.try_into()?
        })
    }
}
*/
impl TryInto<query_es_types::Compare> for Compare {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<query_es_types::Compare, Self::Error> {
        Ok(query_es_types::Compare {
            offset: self.offset,
            unit: self.unit.try_into()?,
        })
    }
}

#[derive(Clone, Serialize, Deserialize)]
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

impl TryInto<query_es_types::QueryAggregate> for QueryAggregate {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<query_es_types::QueryAggregate, Self::Error> {
        Ok(match self {
            QueryAggregate::Min => query_es_types::QueryAggregate::Min,
            QueryAggregate::Max => query_es_types::QueryAggregate::Max,
            QueryAggregate::Sum => query_es_types::QueryAggregate::Sum,
            QueryAggregate::Avg => query_es_types::QueryAggregate::Avg,
            QueryAggregate::Median => query_es_types::QueryAggregate::Median,
            QueryAggregate::DistinctCount => query_es_types::QueryAggregate::DistinctCount,
            QueryAggregate::Percentile25th => query_es_types::QueryAggregate::Percentile25th,
            QueryAggregate::Percentile75th => query_es_types::QueryAggregate::Percentile75th,
            QueryAggregate::Percentile90th => query_es_types::QueryAggregate::Percentile90th,
            QueryAggregate::Percentile99th => query_es_types::QueryAggregate::Percentile99th,
        })
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum QueryAggregatePerGroup {
    Min,
    Max,
    Sum,
    Avg,
    Median,
    DistinctCount,
}

impl TryInto<query_es_types::QueryAggregatePerGroup> for QueryAggregatePerGroup {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<query_es_types::QueryAggregatePerGroup, Self::Error> {
        Ok(match self {
            QueryAggregatePerGroup::Min => query_es_types::QueryAggregatePerGroup::Min,
            QueryAggregatePerGroup::Max => query_es_types::QueryAggregatePerGroup::Max,
            QueryAggregatePerGroup::Sum => query_es_types::QueryAggregatePerGroup::Sum,
            QueryAggregatePerGroup::Avg => query_es_types::QueryAggregatePerGroup::Avg,
            QueryAggregatePerGroup::Median => query_es_types::QueryAggregatePerGroup::Median,
            QueryAggregatePerGroup::DistinctCount => {
                query_es_types::QueryAggregatePerGroup::DistinctCount
            }
        })
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum QueryPerGroup {
    CountEvents,
}

impl TryInto<query_es_types::QueryPerGroup> for QueryPerGroup {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<query_es_types::QueryPerGroup, Self::Error> {
        Ok(match self {
            QueryPerGroup::CountEvents => query_es_types::QueryPerGroup::CountEvents,
        })
    }
}

#[derive(Clone, Serialize, Deserialize)]
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
    QueryFormula {
        formula: String,
    },
}

impl TryInto<query_es_types::Query> for &Query {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<query_es_types::Query, Self::Error> {
        Ok(match self {
            Query::CountEvents => query_es_types::Query::CountEvents,
            Query::CountUniqueGroups => query_es_types::Query::CountUniqueGroups,
            Query::DailyActiveGroups => query_es_types::Query::DailyActiveGroups,
            Query::WeeklyActiveGroups => query_es_types::Query::WeeklyActiveGroups,
            Query::MonthlyActiveGroups => query_es_types::Query::MonthlyActiveGroups,
            Query::CountPerGroup { aggregate } => query_es_types::Query::CountPerGroup {
                aggregate: aggregate.try_into()?,
            },
            Query::AggregatePropertyPerGroup {
                property,
                aggregate_per_group,
                aggregate,
            } => query_es_types::Query::AggregatePropertyPerGroup {
                property: property.to_owned().try_into()?,
                aggregate_per_group: aggregate_per_group.try_into()?,
                aggregate: aggregate.try_into()?,
            },
            Query::AggregateProperty {
                property,
                aggregate,
            } => query_es_types::Query::AggregateProperty {
                property: property.to_owned().try_into()?,
                aggregate: aggregate.try_into()?,
            },
            Query::QueryFormula { formula } => query_es_types::Query::QueryFormula {
                formula: formula.clone(),
            },
        })
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PropertyType {
    User,
    Event,
    Custom,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum EventFilter {
    #[serde(rename_all = "camelCase")]
    Property {
        #[serde(flatten)]
        property: PropertyRef,
        operation: PropValueOperation,
        #[serde(skip_serializing_if = "Option::is_none")]
        value: Option<Vec<Value>>,
    },
}

impl TryInto<query_es_types::EventFilter> for &EventFilter {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<query_es_types::EventFilter, Self::Error> {
        Ok(match self {
            EventFilter::Property {
                property,
                operation,
                value,
            } => query_es_types::EventFilter::Property {
                property: property.to_owned().try_into()?,
                operation: operation.to_owned().try_into()?,
                value: match value {
                    None => None,
                    Some(v) => Some(v.iter().map(json_value_to_scalar).collect::<Result<_>>()?),
                },
            },
        })
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum Breakdown {
    Property {
        #[serde(flatten)]
        property: PropertyRef,
    },
}

impl TryInto<query_es_types::Breakdown> for &Breakdown {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<query_es_types::Breakdown, Self::Error> {
        Ok(match self {
            Breakdown::Property { property } => {
                query_es_types::Breakdown::Property(property.to_owned().try_into()?)
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

#[derive(Clone, Serialize, Deserialize)]
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

impl TryInto<query_es_types::Event> for &Event {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<query_es_types::Event, Self::Error> {
        Ok(query_es_types::Event {
            event: self.event.clone().try_into()?,
            filters: self
                .filters
                .as_ref()
                .map(|v| {
                    v.iter()
                        .map(|v| v.try_into())
                        .collect::<std::result::Result<_, _>>()
                })
                .transpose()?,
            breakdowns: self
                .breakdowns
                .as_ref()
                .map(|v| {
                    v.iter()
                        .map(|v| v.try_into())
                        .collect::<std::result::Result<_, _>>()
                })
                .transpose()?,
            queries: self
                .queries
                .iter()
                .map(|v| v.try_into())
                .collect::<std::result::Result<Vec<query_es_types::Query>, _>>()?
                .iter()
                .enumerate()
                .map(|(idx, v)| NamedQuery::new(v.clone(), Some(self.event.name(idx))))
                .collect(),
        })
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum SegmentCondition {}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Segment {
    name: String,
    conditions: Vec<SegmentCondition>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventSegmentation {
    pub time: QueryTime,
    pub group: String,
    pub interval_unit: TimeUnit,
    pub chart_type: ChartType,
    pub analysis: Analysis,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compare: Option<Compare>,
    pub events: Vec<Event>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filters: Option<Vec<EventFilter>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub breakdowns: Option<Vec<Breakdown>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub segments: Option<Vec<Segment>>,
}

impl TryInto<query_es_types::EventSegmentation> for EventSegmentation {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<query_es_types::EventSegmentation, Self::Error> {
        Ok(query_es_types::EventSegmentation {
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
            filters: self
                .filters
                .map(|v| {
                    v.iter()
                        .map(|v| v.try_into())
                        .collect::<std::result::Result<_, _>>()
                })
                .transpose()?,
            breakdowns: self
                .breakdowns
                .map(|v| {
                    v.iter()
                        .map(|v| v.try_into())
                        .collect::<std::result::Result<_, _>>()
                })
                .transpose()?,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::error::Result;
    use crate::queries::event_segmentation::{
        AggregateFunction, Analysis, Breakdown, ChartType, Compare, Event, EventFilter,
        EventSegmentation, EventType, PartitionedAggregateFunction, PropValueOperation,
        PropertyType, Query, QueryTime, TimeUnit,
    };
    use chrono::{DateTime, Utc};

    use crate::queries::types::{EventRef, PropertyRef};
    use query::event_fields;
    use query::queries::event_segmentation::types::EventSegmentation as QueryEventSegmentation;
    use serde_json::json;

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
            interval_unit: TimeUnit::Minute,
            chart_type: ChartType::Line,
            analysis: Analysis::Linear,
            compare: Some(Compare {
                offset: 1,
                unit: TimeUnit::Second,
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
            filters: Some(vec![EventFilter::Property {
                property: PropertyRef::User {
                    property_name: "p1".to_string(),
                },
                operation: PropValueOperation::Eq,
                value: Some(vec![json!(true)]),
            }]),
            breakdowns: Some(vec![Breakdown::Property {
                property: PropertyRef::User {
                    property_name: "Device".to_string(),
                },
            }]),
            segments: None,
        };

        let _qes: QueryEventSegmentation = es.clone().try_into()?;
        let j = serde_json::to_string_pretty(&es).unwrap();
        print!("1 {}", j);

        Ok(())
    }
}
