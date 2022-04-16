use chrono::{DateTime, Duration, Utc};
use datafusion_expr::Operator;
use serde::{Deserialize, Serialize};
use std::ops::Sub;
use datafusion::physical_plan::aggregates::AggregateFunction as DFAggregateFunction;
use crate::physical_plan::expressions::partitioned_aggregate;

#[derive(Clone, Serialize, Deserialize)]
pub enum QueryTime {
    Between {
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    },
    From(DateTime<Utc>),
    Last {
        last: i64,
        unit: TimeUnit,
    },
}

#[derive(Clone, Serialize, Deserialize)]
pub enum TimeUnit {
    Second,
    Minute,
    Hour,
    Day,
    Week,
    Month,
    Year,
}

impl TimeUnit {
    pub fn duration(&self, n: i64) -> Duration {
        match self {
            TimeUnit::Second => Duration::seconds(n),
            TimeUnit::Minute => Duration::minutes(n),
            TimeUnit::Hour => Duration::hours(n),
            TimeUnit::Day => Duration::days(n),
            TimeUnit::Week => Duration::weeks(n),
            TimeUnit::Month => Duration::days(n) * 30,
            TimeUnit::Year => Duration::days(n) * 365,
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub enum PropertyRef {
    User(String),
    UserCustom(String),
    Event(String),
    EventCustom(String),
}

impl PropertyRef {
    pub fn name(&self) -> String {
        match self {
            PropertyRef::User(name) => name.clone(),
            PropertyRef::UserCustom(name) => name.clone(),
            PropertyRef::Event(name) => name.clone(),
            PropertyRef::EventCustom(name) => name.clone(),
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub enum PropValueOperation {
    Eq,
    Neq,
    IsNull,
    IsNotNull,
}

impl Into<Operator> for PropValueOperation {
    fn into(self) -> Operator {
        match self {
            PropValueOperation::Eq => Operator::Eq,
            PropValueOperation::Neq => Operator::NotEq,
            _ => panic!("unreachable"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash, Serialize, Deserialize)]
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

impl From<&AggregateFunction> for DFAggregateFunction {
    fn from(f: &AggregateFunction) -> Self {
        match f {
            AggregateFunction::Count => DFAggregateFunction::Count,
            AggregateFunction::Sum => DFAggregateFunction::Sum,
            AggregateFunction::Min => DFAggregateFunction::Min,
            AggregateFunction::Max => DFAggregateFunction::Max,
            AggregateFunction::Avg => DFAggregateFunction::Avg,
            AggregateFunction::ApproxDistinct => DFAggregateFunction::ApproxDistinct,
            AggregateFunction::ArrayAgg => DFAggregateFunction::ArrayAgg,
            AggregateFunction::Variance => DFAggregateFunction::Variance,
            AggregateFunction::VariancePop => DFAggregateFunction::VariancePop,
            AggregateFunction::Stddev => DFAggregateFunction::Stddev,
            AggregateFunction::StddevPop => DFAggregateFunction::StddevPop,
            AggregateFunction::Covariance => DFAggregateFunction::Covariance,
            AggregateFunction::CovariancePop => DFAggregateFunction::CovariancePop,
            AggregateFunction::Correlation => DFAggregateFunction::Correlation,
            AggregateFunction::ApproxPercentileCont => DFAggregateFunction::ApproxPercentileCont,
            AggregateFunction::ApproxMedian => DFAggregateFunction::ApproxMedian,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash, Serialize, Deserialize)]
pub enum PartitionedAggregateFunction {
    Count,
    Sum,
}

impl From<&PartitionedAggregateFunction> for partitioned_aggregate::PartitionedAggregateFunction {
    fn from(f: &PartitionedAggregateFunction) -> Self {
        match f {
            PartitionedAggregateFunction::Count => partitioned_aggregate::PartitionedAggregateFunction::Count,
            PartitionedAggregateFunction::Sum => partitioned_aggregate::PartitionedAggregateFunction::Sum,
        }
    }
}