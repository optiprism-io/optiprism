use chrono::DateTime;
use chrono::Utc;

use crate::queries::event_segmentation::EventSegmentation;
use crate::queries::property_values::ListPropertyValuesRequest;
use crate::Context;
use crate::ListResponse;
use crate::PlatformError;
use crate::QueryResponse;

pub mod event_segmentation;
pub mod property_values;
pub mod queries;

use axum::async_trait;
pub use queries::Queries;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

use crate::Result;

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

impl TryInto<common::query::QueryTime> for QueryTime {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<common::query::QueryTime, Self::Error> {
        Ok(match self {
            QueryTime::Between { from, to } => common::query::QueryTime::Between { from, to },
            QueryTime::From { from } => common::query::QueryTime::From(from),
            QueryTime::Last { last, unit } => common::query::QueryTime::Last {
                last,
                unit: unit.try_into()?,
            },
        })
    }
}

impl TryInto<QueryTime> for common::query::QueryTime {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<QueryTime, Self::Error> {
        Ok(match self {
            common::query::QueryTime::Between { from, to } => QueryTime::Between { from, to },
            common::query::QueryTime::From(from) => QueryTime::From { from },
            common::query::QueryTime::Last { last, unit } => QueryTime::Last {
                last,
                unit: unit.try_into()?,
            },
        })
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum SegmentTime {
    Between {
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    },
    WindowEach {
        unit: TimeIntervalUnit,
    },
    Last {
        last: i64,
        unit: TimeIntervalUnit,
    },
    AfterFirstUse {
        within: i64,
        unit: TimeIntervalUnit,
    },
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

impl TryInto<common::query::TimeIntervalUnit> for TimeIntervalUnit {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<common::query::TimeIntervalUnit, Self::Error> {
        Ok(match self {
            TimeIntervalUnit::Second => common::query::TimeIntervalUnit::Second,
            TimeIntervalUnit::Minute => common::query::TimeIntervalUnit::Minute,
            TimeIntervalUnit::Hour => common::query::TimeIntervalUnit::Hour,
            TimeIntervalUnit::Day => common::query::TimeIntervalUnit::Day,
            TimeIntervalUnit::Week => common::query::TimeIntervalUnit::Week,
            TimeIntervalUnit::Month => common::query::TimeIntervalUnit::Month,
            TimeIntervalUnit::Year => common::query::TimeIntervalUnit::Year,
        })
    }
}

impl TryInto<TimeIntervalUnit> for common::query::TimeIntervalUnit {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<TimeIntervalUnit, Self::Error> {
        Ok(match self {
            common::query::TimeIntervalUnit::Second => TimeIntervalUnit::Second,
            common::query::TimeIntervalUnit::Minute => TimeIntervalUnit::Minute,
            common::query::TimeIntervalUnit::Hour => TimeIntervalUnit::Hour,
            common::query::TimeIntervalUnit::Day => TimeIntervalUnit::Day,
            common::query::TimeIntervalUnit::Week => TimeIntervalUnit::Week,
            common::query::TimeIntervalUnit::Month => TimeIntervalUnit::Month,
            common::query::TimeIntervalUnit::Year => TimeIntervalUnit::Year,
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
