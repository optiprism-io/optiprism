use chrono::DateTime;
use chrono::Utc;

use crate::queries::event_segmentation::EventSegmentation;
use crate::queries::property_values::ListPropertyValuesRequest;
use crate::Context;
use crate::DataTable;
use crate::ListResponse;
use crate::PlatformError;

pub mod event_segmentation;
pub mod property_values;
pub mod provider_impl;

use axum::async_trait;
use common::physical_plan::expressions::partitioned_aggregate::PartitionedAggregateFunction as QueryPartitionedAggregateFunction;
pub use provider_impl::ProviderImpl;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

use crate::Result;

#[async_trait]
pub trait Provider: Sync + Send {
    async fn event_segmentation(
        &self,
        ctx: Context,
        organization_id: u64,
        project_id: u64,
        req: EventSegmentation,
    ) -> Result<DataTable>;

    async fn property_values(
        &self,
        ctx: Context,
        organization_id: u64,
        project_id: u64,
        req: ListPropertyValuesRequest,
    ) -> Result<ListResponse<Value>>;
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

impl TryInto<common::queries::QueryTime> for QueryTime {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<common::queries::QueryTime, Self::Error> {
        Ok(match self {
            QueryTime::Between { from, to } => common::queries::QueryTime::Between { from, to },
            QueryTime::From { from } => common::queries::QueryTime::From(from),
            QueryTime::Last { last, unit } => common::queries::QueryTime::Last {
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

impl TryInto<common::queries::TimeIntervalUnit> for TimeIntervalUnit {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<common::queries::TimeIntervalUnit, Self::Error> {
        Ok(match self {
            TimeIntervalUnit::Second => common::queries::TimeIntervalUnit::Second,
            TimeIntervalUnit::Minute => common::queries::TimeIntervalUnit::Minute,
            TimeIntervalUnit::Hour => common::queries::TimeIntervalUnit::Hour,
            TimeIntervalUnit::Day => common::queries::TimeIntervalUnit::Day,
            TimeIntervalUnit::Week => common::queries::TimeIntervalUnit::Week,
            TimeIntervalUnit::Month => common::queries::TimeIntervalUnit::Month,
            TimeIntervalUnit::Year => common::queries::TimeIntervalUnit::Year,
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

impl TryInto<datafusion::physical_plan::aggregates::AggregateFunction> for &AggregateFunction {
    type Error = PlatformError;

    fn try_into(
        self,
    ) -> std::result::Result<datafusion::physical_plan::aggregates::AggregateFunction, Self::Error>
    {
        Ok(match self {
            AggregateFunction::Count => {
                datafusion::physical_plan::aggregates::AggregateFunction::Count
            }
            AggregateFunction::Sum => datafusion::physical_plan::aggregates::AggregateFunction::Sum,
            AggregateFunction::Min => datafusion::physical_plan::aggregates::AggregateFunction::Min,
            AggregateFunction::Max => datafusion::physical_plan::aggregates::AggregateFunction::Max,
            AggregateFunction::Avg => datafusion::physical_plan::aggregates::AggregateFunction::Avg,
            AggregateFunction::ApproxDistinct => {
                datafusion::physical_plan::aggregates::AggregateFunction::ApproxDistinct
            }
            AggregateFunction::ArrayAgg => {
                datafusion::physical_plan::aggregates::AggregateFunction::ArrayAgg
            }
            AggregateFunction::Variance => {
                datafusion::physical_plan::aggregates::AggregateFunction::Variance
            }
            AggregateFunction::VariancePop => {
                datafusion::physical_plan::aggregates::AggregateFunction::VariancePop
            }
            AggregateFunction::Stddev => {
                datafusion::physical_plan::aggregates::AggregateFunction::Stddev
            }
            AggregateFunction::StddevPop => {
                datafusion::physical_plan::aggregates::AggregateFunction::StddevPop
            }
            AggregateFunction::Covariance => {
                datafusion::physical_plan::aggregates::AggregateFunction::Covariance
            }
            AggregateFunction::CovariancePop => {
                datafusion::physical_plan::aggregates::AggregateFunction::CovariancePop
            }
            AggregateFunction::Correlation => {
                datafusion::physical_plan::aggregates::AggregateFunction::Correlation
            }
            AggregateFunction::ApproxPercentileCont => {
                datafusion::physical_plan::aggregates::AggregateFunction::ApproxPercentileCont
            }
            AggregateFunction::ApproxMedian => {
                datafusion::physical_plan::aggregates::AggregateFunction::ApproxMedian
            }
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

impl TryInto<QueryPartitionedAggregateFunction> for &PartitionedAggregateFunction {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<QueryPartitionedAggregateFunction, Self::Error> {
        Ok(match self {
            PartitionedAggregateFunction::Count => QueryPartitionedAggregateFunction::Count,
            PartitionedAggregateFunction::Sum => QueryPartitionedAggregateFunction::Sum,
            _ => todo!(),
        })
    }
}
