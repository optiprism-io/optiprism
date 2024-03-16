use chrono::DateTime;
use chrono::Utc;

use crate::json_value_to_scalar;
use crate::scalar_to_json_value;
use crate::EventFilter;
use crate::EventRef;
use crate::PlatformError;
use crate::PropValueOperation;
use crate::PropertyRef;

pub mod event_records_search;
pub mod event_segmentation;
pub mod funnel;
pub mod property_values;
pub mod provider;
mod validation;

pub use provider::Queries;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

use crate::queries::event_segmentation::QueryAggregate;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum QueryResponseFormat {
    Json,
    JsonCompact,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct QueryParams {
    format: Option<QueryResponseFormat>,
    timestamp: Option<i64>,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum QueryTime {
    Between {
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    },
    From {
        from: DateTime<Utc>,
    },
    Last {
        last: i64,
        unit: TimeIntervalUnit,
    },
}

impl Into<common::query::QueryTime> for QueryTime {
    fn into(self) -> common::query::QueryTime {
        match self {
            QueryTime::Between { from, to } => common::query::QueryTime::Between { from, to },
            QueryTime::From { from } => common::query::QueryTime::From(from),
            QueryTime::Last { last, unit } => common::query::QueryTime::Last {
                last,
                unit: unit.into(),
            },
        }
    }
}

impl Into<QueryTime> for common::query::QueryTime {
    fn into(self) -> QueryTime {
        match self {
            common::query::QueryTime::Between { from, to } => QueryTime::Between { from, to },
            common::query::QueryTime::From(from) => QueryTime::From { from },
            common::query::QueryTime::Last { last, unit } => QueryTime::Last {
                last,
                unit: unit.into(),
            },
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum TimeIntervalUnit {
    Second,
    Minute,
    Hour,
    Day,
    Week,
    Month,
    Year,
}

impl Into<common::query::TimeIntervalUnit> for TimeIntervalUnit {
    fn into(self) -> common::query::TimeIntervalUnit {
        match self {
            TimeIntervalUnit::Second => common::query::TimeIntervalUnit::Second,
            TimeIntervalUnit::Minute => common::query::TimeIntervalUnit::Minute,
            TimeIntervalUnit::Hour => common::query::TimeIntervalUnit::Hour,
            TimeIntervalUnit::Day => common::query::TimeIntervalUnit::Day,
            TimeIntervalUnit::Week => common::query::TimeIntervalUnit::Week,
            TimeIntervalUnit::Month => common::query::TimeIntervalUnit::Month,
            TimeIntervalUnit::Year => common::query::TimeIntervalUnit::Year,
        }
    }
}

impl Into<TimeIntervalUnit> for common::query::TimeIntervalUnit {
    fn into(self) -> TimeIntervalUnit {
        match self {
            common::query::TimeIntervalUnit::Second => TimeIntervalUnit::Second,
            common::query::TimeIntervalUnit::Minute => TimeIntervalUnit::Minute,
            common::query::TimeIntervalUnit::Hour => TimeIntervalUnit::Hour,
            common::query::TimeIntervalUnit::Day => TimeIntervalUnit::Day,
            common::query::TimeIntervalUnit::Week => TimeIntervalUnit::Week,
            common::query::TimeIntervalUnit::Month => TimeIntervalUnit::Month,
            common::query::TimeIntervalUnit::Year => TimeIntervalUnit::Year,
        }
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

impl TryInto<common::query::Breakdown> for &Breakdown {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<common::query::Breakdown, Self::Error> {
        Ok(match self {
            Breakdown::Property { property } => {
                common::query::Breakdown::Property(property.to_owned().try_into()?)
            }
        })
    }
}

impl TryInto<Breakdown> for &common::query::Breakdown {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<Breakdown, Self::Error> {
        Ok(match self {
            common::query::Breakdown::Property(property) => Breakdown::Property {
                property: property.to_owned().try_into()?,
            },
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum AggregateFunction {
    /// count
    Count,
    /// sum
    Sum,
    /// min
    Min,
    /// max
    Max,
    /// avg
    Avg,
    /// Approximate aggregate function
    ApproxDistinct,
    /// array_agg
    ArrayAgg,
    /// Variance (Sample)
    Variance,
    /// Variance (Population)
    VariancePop,
    /// Standard Deviation (Sample)
    Stddev,
    /// Standard Deviation (Population)
    StddevPop,
    /// Covariance (Sample)
    Covariance,
    /// Covariance (Population)
    CovariancePop,
    /// Correlation
    Correlation,
    /// Approximate continuous percentile function
    ApproxPercentileCont,
    /// ApproxMedian
    ApproxMedian,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum DidEventAggregate {
    Count {
        operation: PropValueOperation,
        value: i64,
        time: crate::queries::SegmentTime,
    },
    RelativeCount {
        #[serde(flatten)]
        event: EventRef,
        operation: PropValueOperation,
        filters: Option<Vec<EventFilter>>,
        time: crate::queries::SegmentTime,
    },
    AggregateProperty {
        #[serde(flatten)]
        property: PropertyRef,
        aggregate: QueryAggregate,
        operation: PropValueOperation,
        value: Option<Value>,
        time: crate::queries::SegmentTime,
    },
    HistoricalCount {
        operation: PropValueOperation,
        value: u64,
        time: crate::queries::SegmentTime,
    },
}

impl TryInto<common::query::DidEventAggregate> for DidEventAggregate {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<common::query::DidEventAggregate, Self::Error> {
        Ok(match self {
            DidEventAggregate::Count {
                operation,
                value,
                time,
            } => common::query::DidEventAggregate::Count {
                operation: operation.try_into()?,
                value,
                time: time.try_into()?,
            },
            DidEventAggregate::RelativeCount {
                event,
                operation,
                filters,
                time,
            } => common::query::DidEventAggregate::RelativeCount {
                event: event.into(),
                operation: operation.try_into()?,
                filters: filters
                    .map_or_else(
                        || None,
                        |v| {
                            if v.is_empty() {
                                None
                            } else {
                                Some(v.iter().map(|v| v.try_into()).collect::<crate::Result<_>>())
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
            } => common::query::DidEventAggregate::AggregateProperty {
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
            } => common::query::DidEventAggregate::HistoricalCount {
                operation: operation.try_into()?,
                value,
                time: time.try_into()?,
            },
        })
    }
}

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

impl TryInto<common::query::SegmentTime> for SegmentTime {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<common::query::SegmentTime, Self::Error> {
        Ok(match self {
            SegmentTime::Between { from, to } => common::query::SegmentTime::Between { from, to },
            SegmentTime::From(v) => common::query::SegmentTime::From(v),
            SegmentTime::Last { last: n, unit } => common::query::SegmentTime::Last {
                n,
                unit: unit.try_into()?,
            },
            SegmentTime::AfterFirstUse { within, unit } => {
                common::query::SegmentTime::AfterFirstUse {
                    within,
                    unit: unit.try_into()?,
                }
            }
            SegmentTime::WindowEach { unit, n } => common::query::SegmentTime::Each {
                n,
                unit: unit.try_into()?,
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
        time: crate::queries::SegmentTime,
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

impl TryInto<common::query::SegmentCondition> for SegmentCondition {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<common::query::SegmentCondition, Self::Error> {
        Ok(match self {
            SegmentCondition::HasPropertyValue {
                property_name,
                operation,
                value,
            } => common::query::SegmentCondition::HasPropertyValue {
                property_name,
                operation: operation.try_into()?,
                value: match value {
                    Some(v) if !v.is_empty() => Some(
                        v.iter()
                            .map(json_value_to_scalar)
                            .collect::<crate::Result<_>>()?,
                    ),
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
            } => common::query::SegmentCondition::HadPropertyValue {
                property_name,
                operation: operation.try_into()?,
                value: match value {
                    Some(v) if !v.is_empty() => Some(
                        v.iter()
                            .map(json_value_to_scalar)
                            .collect::<crate::Result<_>>()?,
                    ),
                    _ => None,
                },
                time: time.try_into()?,
            },
            SegmentCondition::DidEvent {
                event,
                filters,
                aggregate,
            } => common::query::SegmentCondition::DidEvent {
                event: event.into(),
                filters: filters
                    .map_or_else(
                        || None,
                        |v| {
                            if v.is_empty() {
                                None
                            } else {
                                Some(v.iter().map(|v| v.try_into()).collect::<crate::Result<_>>())
                            }
                        },
                    )
                    .transpose()?,
                aggregate: aggregate.try_into()?,
            },
        })
    }
}

impl TryInto<crate::queries::SegmentTime> for common::query::SegmentTime {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<crate::queries::SegmentTime, Self::Error> {
        Ok(match self {
            common::query::SegmentTime::Between { from, to } => {
                crate::queries::SegmentTime::Between { from, to }
            }
            common::query::SegmentTime::From(from) => crate::queries::SegmentTime::From(from),
            common::query::SegmentTime::Last { n, unit } => crate::queries::SegmentTime::Last {
                last: n,
                unit: unit.try_into()?,
            },
            common::query::SegmentTime::AfterFirstUse { within, unit } => {
                crate::queries::SegmentTime::AfterFirstUse {
                    within,
                    unit: unit.try_into()?,
                }
            }
            common::query::SegmentTime::Each { n, unit } => {
                crate::queries::SegmentTime::WindowEach {
                    unit: unit.try_into()?,
                    n,
                }
            }
        })
    }
}

impl TryInto<DidEventAggregate> for common::query::DidEventAggregate {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<DidEventAggregate, Self::Error> {
        Ok(match self {
            common::query::DidEventAggregate::Count {
                operation,
                value,
                time,
            } => DidEventAggregate::Count {
                operation: operation.try_into()?,
                value,
                time: time.try_into()?,
            },
            common::query::DidEventAggregate::RelativeCount {
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
                                Some(v.iter().map(|v| v.try_into()).collect::<crate::Result<_>>())
                            }
                        },
                    )
                    .transpose()?,
                time: time.try_into()?,
            },
            common::query::DidEventAggregate::AggregateProperty {
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
            common::query::DidEventAggregate::HistoricalCount {
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

impl TryInto<SegmentCondition> for common::query::SegmentCondition {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<SegmentCondition, Self::Error> {
        Ok(match self {
            common::query::SegmentCondition::HasPropertyValue {
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
                                Some(
                                    v.iter()
                                        .map(scalar_to_json_value)
                                        .collect::<crate::Result<_>>(),
                                )
                            }
                        },
                    )
                    .transpose()?,
            },
            common::query::SegmentCondition::HadPropertyValue {
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
                                Some(
                                    v.iter()
                                        .map(scalar_to_json_value)
                                        .collect::<crate::Result<_>>(),
                                )
                            }
                        },
                    )
                    .transpose()?,

                time: time.try_into()?,
            },
            common::query::SegmentCondition::DidEvent {
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
                                Some(v.iter().map(|v| v.try_into()).collect::<crate::Result<_>>())
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
                        .collect::<crate::Result<Vec<_>>>()
                })
                .collect::<crate::Result<_>>()?,
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
                        .collect::<crate::Result<Vec<_>>>()
                })
                .collect::<crate::Result<_>>()?,
        })
    }
}

impl TryInto<common::query::AggregateFunction> for &AggregateFunction {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<common::query::AggregateFunction, Self::Error> {
        Ok(match self {
            AggregateFunction::Count => common::query::AggregateFunction::Count,
            AggregateFunction::Sum => common::query::AggregateFunction::Sum,
            AggregateFunction::Min => common::query::AggregateFunction::Min,
            AggregateFunction::Max => common::query::AggregateFunction::Max,
            AggregateFunction::Avg => common::query::AggregateFunction::Avg,
            AggregateFunction::ApproxDistinct => common::query::AggregateFunction::ApproxDistinct,
            AggregateFunction::ArrayAgg => common::query::AggregateFunction::ArrayAgg,
            AggregateFunction::Variance => common::query::AggregateFunction::Variance,
            AggregateFunction::VariancePop => common::query::AggregateFunction::VariancePop,
            AggregateFunction::Stddev => common::query::AggregateFunction::Stddev,
            AggregateFunction::StddevPop => common::query::AggregateFunction::StddevPop,
            AggregateFunction::Covariance => common::query::AggregateFunction::Covariance,
            AggregateFunction::CovariancePop => common::query::AggregateFunction::CovariancePop,
            AggregateFunction::Correlation => common::query::AggregateFunction::Correlation,
            AggregateFunction::ApproxPercentileCont => {
                common::query::AggregateFunction::ApproxPercentileCont
            }
            AggregateFunction::ApproxMedian => common::query::AggregateFunction::ApproxMedian,
        })
    }
}

impl TryInto<AggregateFunction> for common::query::AggregateFunction {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<AggregateFunction, Self::Error> {
        Ok(match self {
            common::query::AggregateFunction::Count => AggregateFunction::Count,
            common::query::AggregateFunction::Sum => AggregateFunction::Sum,
            common::query::AggregateFunction::Min => AggregateFunction::Min,
            common::query::AggregateFunction::Max => AggregateFunction::Max,
            common::query::AggregateFunction::Avg => AggregateFunction::Avg,
            common::query::AggregateFunction::ApproxDistinct => AggregateFunction::ApproxDistinct,
            common::query::AggregateFunction::ArrayAgg => AggregateFunction::ArrayAgg,
            common::query::AggregateFunction::Variance => AggregateFunction::Variance,
            common::query::AggregateFunction::VariancePop => AggregateFunction::VariancePop,
            common::query::AggregateFunction::Stddev => AggregateFunction::Stddev,
            common::query::AggregateFunction::StddevPop => AggregateFunction::StddevPop,
            common::query::AggregateFunction::Covariance => AggregateFunction::Covariance,
            common::query::AggregateFunction::CovariancePop => AggregateFunction::CovariancePop,
            common::query::AggregateFunction::Correlation => AggregateFunction::Correlation,
            common::query::AggregateFunction::ApproxPercentileCont => {
                AggregateFunction::ApproxPercentileCont
            }
            common::query::AggregateFunction::ApproxMedian => AggregateFunction::ApproxMedian,
            _ => return Err(PlatformError::BadRequest("unimplemented".to_string())),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PartitionedAggregateFunction {
    Sum,
    Avg,
    Median,
    Count,
    Min,
    Max,
    DistinctCount,
    Percentile25,
    Percentile75,
    Percentile90,
    Percentile99,
}

impl TryInto<common::query::PartitionedAggregateFunction> for &PartitionedAggregateFunction {
    type Error = PlatformError;

    fn try_into(
        self,
    ) -> std::result::Result<common::query::PartitionedAggregateFunction, Self::Error> {
        Ok(match self {
            PartitionedAggregateFunction::Count => {
                common::query::PartitionedAggregateFunction::Count
            }
            PartitionedAggregateFunction::Sum => common::query::PartitionedAggregateFunction::Sum,
            PartitionedAggregateFunction::Avg => common::query::PartitionedAggregateFunction::Avg,
            PartitionedAggregateFunction::Min => common::query::PartitionedAggregateFunction::Min,
            PartitionedAggregateFunction::Max => common::query::PartitionedAggregateFunction::Max,
            _ => todo!(),
        })
    }
}

impl TryInto<PartitionedAggregateFunction> for common::query::PartitionedAggregateFunction {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<PartitionedAggregateFunction, Self::Error> {
        Ok(match self {
            common::query::PartitionedAggregateFunction::Count => {
                PartitionedAggregateFunction::Count
            }
            common::query::PartitionedAggregateFunction::Sum => PartitionedAggregateFunction::Sum,
            common::query::PartitionedAggregateFunction::Avg => PartitionedAggregateFunction::Avg,
            common::query::PartitionedAggregateFunction::Min => PartitionedAggregateFunction::Min,
            common::query::PartitionedAggregateFunction::Max => PartitionedAggregateFunction::Max,
        })
    }
}
