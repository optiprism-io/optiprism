pub mod event_segmentation;

use chrono::DateTime;
use chrono::Utc;
use chronoutil::RelativeDuration;
use serde::Deserialize;
use serde::Serialize;

use crate::error::CommonError;

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
    /// median
    Median,
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
    /// Approximate continuous percentile function with weight
    ApproxPercentileContWithWeight,
    /// ApproxMedian
    ApproxMedian,
    /// Grouping
    Grouping,
}

// impl TryInto<datafusion::physical_plan::aggregates::AggregateFunction> for &AggregateFunction {
//     // type Error = PlatformError;
//     type Error = CommonError;

//     fn try_into(
//         self,
//     ) -> std::result::Result<datafusion::physical_plan::aggregates::AggregateFunction, Self::Error>
//     {
//         Ok(match self {
//             AggregateFunction::Count => {
//                 datafusion::physical_plan::aggregates::AggregateFunction::Count
//             }
//             AggregateFunction::Sum => datafusion::physical_plan::aggregates::AggregateFunction::Sum,
//             AggregateFunction::Min => datafusion::physical_plan::aggregates::AggregateFunction::Min,
//             AggregateFunction::Max => datafusion::physical_plan::aggregates::AggregateFunction::Max,
//             AggregateFunction::Avg => datafusion::physical_plan::aggregates::AggregateFunction::Avg,
//             AggregateFunction::ApproxDistinct => {
//                 datafusion::physical_plan::aggregates::AggregateFunction::ApproxDistinct
//             }
//             AggregateFunction::ArrayAgg => {
//                 datafusion::physical_plan::aggregates::AggregateFunction::ArrayAgg
//             }
//             AggregateFunction::Variance => {
//                 datafusion::physical_plan::aggregates::AggregateFunction::Variance
//             }
//             AggregateFunction::VariancePop => {
//                 datafusion::physical_plan::aggregates::AggregateFunction::VariancePop
//             }
//             AggregateFunction::Stddev => {
//                 datafusion::physical_plan::aggregates::AggregateFunction::Stddev
//             }
//             AggregateFunction::StddevPop => {
//                 datafusion::physical_plan::aggregates::AggregateFunction::StddevPop
//             }
//             AggregateFunction::Covariance => {
//                 datafusion::physical_plan::aggregates::AggregateFunction::Covariance
//             }
//             AggregateFunction::CovariancePop => {
//                 datafusion::physical_plan::aggregates::AggregateFunction::CovariancePop
//             }
//             AggregateFunction::Correlation => {
//                 datafusion::physical_plan::aggregates::AggregateFunction::Correlation
//             }
//             AggregateFunction::ApproxPercentileCont => {
//                 datafusion::physical_plan::aggregates::AggregateFunction::ApproxPercentileCont
//             }
//             AggregateFunction::ApproxMedian => {
//                 datafusion::physical_plan::aggregates::AggregateFunction::ApproxMedian
//             }
//             AggregateFunction::Median => {
//                 datafusion::physical_plan::aggregates::AggregateFunction::Median
//             }
//             AggregateFunction::Grouping => {
//                 datafusion::physical_plan::aggregates::AggregateFunction::Grouping
//             }
//             AggregateFunction::ApproxPercentileContWithWeight => {
//                 datafusion::physical_plan::aggregates::AggregateFunction::ApproxPercentileContWithWeight
//             }
//         })
//     }
// }

impl TryFrom<AggregateFunction> for datafusion::physical_plan::aggregates::AggregateFunction {
    // type Error = PlatformError;
    type Error = CommonError;

    fn try_from(
        f: AggregateFunction,
    ) -> std::result::Result<datafusion::physical_plan::aggregates::AggregateFunction, Self::Error>
    {
        Ok(match f {
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
            AggregateFunction::Median => {
                datafusion::physical_plan::aggregates::AggregateFunction::Median
            }
            AggregateFunction::Grouping => {
                datafusion::physical_plan::aggregates::AggregateFunction::Grouping
            }
            AggregateFunction::ApproxPercentileContWithWeight => {
                datafusion::physical_plan::aggregates::AggregateFunction::ApproxPercentileContWithWeight
            }
        })
    }
}

// impl TryFrom<AggregateFunction> for datafusion_expr::AggregateFunction {
//     // type Error = PlatformError;
//     type Error = CommonError;

//     fn try_from(f: datafusion_expr::AggregateFunction) -> std::result::Result<Self, Self::Error> {
//         Ok(match f {
//             datafusion_expr::AggregateFunction::Count => AggregateFunction::Count,
//             datafusion_expr::AggregateFunction::Sum => AggregateFunction::Sum,
//             datafusion_expr::AggregateFunction::Min => AggregateFunction::Min,
//             datafusion_expr::AggregateFunction::Max => AggregateFunction::Max,
//             datafusion_expr::AggregateFunction::Avg => AggregateFunction::Avg,
//             datafusion_expr::AggregateFunction::Median => AggregateFunction::Median,
//             datafusion_expr::AggregateFunction::ApproxDistinct => AggregateFunction::ApproxDistinct,
//             datafusion_expr::AggregateFunction::ArrayAgg => AggregateFunction::ArrayAgg,
//             datafusion_expr::AggregateFunction::Variance => AggregateFunction::Variance,
//             datafusion_expr::AggregateFunction::VariancePop => AggregateFunction::VariancePop,
//             datafusion_expr::AggregateFunction::Stddev => AggregateFunction::Stddev,
//             datafusion_expr::AggregateFunction::StddevPop => AggregateFunction::StddevPop,
//             datafusion_expr::AggregateFunction::Covariance => AggregateFunction::Covariance,
//             datafusion_expr::AggregateFunction::CovariancePop => AggregateFunction::CovariancePop,
//             datafusion_expr::AggregateFunction::Correlation => AggregateFunction::Correlation,
//             datafusion_expr::AggregateFunction::ApproxPercentileCont => {
//                 AggregateFunction::ApproxPercentileCont
//             }
//             datafusion_expr::AggregateFunction::ApproxPercentileContWithWeight => {
//                 AggregateFunction::ApproxPercentileContWithWeight
//             }
//             datafusion_expr::AggregateFunction::ApproxMedian => AggregateFunction::ApproxMedian,
//             datafusion_expr::AggregateFunction::Grouping => AggregateFunction::Grouping,
//         })
//     }
// }

// #[derive(Clone, Debug)]
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum QueryTime {
    Between {
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    },
    From(DateTime<Utc>),
    Last {
        last: i64,
        unit: TimeIntervalUnit,
    },
}

impl QueryTime {
    pub fn range(&self, cur_time: DateTime<Utc>) -> (DateTime<Utc>, DateTime<Utc>) {
        match self {
            QueryTime::Between { from, to } => (*from, *to),
            QueryTime::From(from) => (*from, cur_time),
            QueryTime::Last { last, unit } => (cur_time + unit.relative_duration(-*last), cur_time),
        }
    }
}

// #[derive(Clone, Debug)]
#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
pub enum TimeIntervalUnit {
    Second,
    Minute,
    Hour,
    Day,
    Week,
    Month,
    Year,
}

impl TimeIntervalUnit {
    pub fn relative_duration(&self, n: i64) -> RelativeDuration {
        match self {
            TimeIntervalUnit::Second => RelativeDuration::seconds(n),
            TimeIntervalUnit::Minute => RelativeDuration::minutes(n),
            TimeIntervalUnit::Hour => RelativeDuration::hours(n),
            TimeIntervalUnit::Day => RelativeDuration::days(n),
            TimeIntervalUnit::Week => RelativeDuration::weeks(n),
            TimeIntervalUnit::Month => RelativeDuration::months(n as i32),
            TimeIntervalUnit::Year => RelativeDuration::years(n as i32),
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            TimeIntervalUnit::Second => "second",
            TimeIntervalUnit::Minute => "minute",
            TimeIntervalUnit::Hour => "hour",
            TimeIntervalUnit::Day => "day",
            TimeIntervalUnit::Week => "week",
            TimeIntervalUnit::Month => "month",
            TimeIntervalUnit::Year => "year",
        }
    }
}
