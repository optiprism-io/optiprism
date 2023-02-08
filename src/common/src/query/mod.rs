use std::fmt;

use chrono::prelude::*;
use chrono::DateTime;
use chrono::Duration;
use chrono::Utc;
use chronoutil::DateRule;
use chronoutil::RelativeDuration;
use datafusion::physical_plan::aggregates::AggregateFunction as DFAggregateFunction;
use datafusion_common::ScalarValue;
use datafusion_expr::Operator as DFOperator;
use serde::Deserialize;
use serde::Serialize;

use crate::error::CommonError;
use crate::error::Result;
use crate::scalar::ScalarValueRef;

pub mod event_segmentation;

/// Enum of all built-in aggregate functions
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
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

impl From<DFAggregateFunction> for AggregateFunction {
    fn from(v: DFAggregateFunction) -> AggregateFunction {
        match v {
            DFAggregateFunction::Count => AggregateFunction::Count,
            DFAggregateFunction::Sum => AggregateFunction::Sum,
            DFAggregateFunction::Min => AggregateFunction::Min,
            DFAggregateFunction::Max => AggregateFunction::Max,
            DFAggregateFunction::Avg => AggregateFunction::Avg,
            DFAggregateFunction::Median => AggregateFunction::Median,
            DFAggregateFunction::ApproxDistinct => AggregateFunction::ApproxDistinct,
            DFAggregateFunction::ArrayAgg => AggregateFunction::ArrayAgg,
            DFAggregateFunction::Variance => AggregateFunction::Variance,
            DFAggregateFunction::VariancePop => AggregateFunction::VariancePop,
            DFAggregateFunction::Stddev => AggregateFunction::Stddev,
            DFAggregateFunction::StddevPop => AggregateFunction::StddevPop,
            DFAggregateFunction::Covariance => AggregateFunction::Covariance,
            DFAggregateFunction::CovariancePop => AggregateFunction::CovariancePop,
            DFAggregateFunction::Correlation => AggregateFunction::Correlation,
            DFAggregateFunction::ApproxPercentileCont => AggregateFunction::ApproxPercentileCont,
            DFAggregateFunction::ApproxPercentileContWithWeight => {
                AggregateFunction::ApproxPercentileContWithWeight
            }
            DFAggregateFunction::ApproxMedian => AggregateFunction::ApproxMedian,
            DFAggregateFunction::Grouping => AggregateFunction::Grouping,
        }
    }
}

impl From<AggregateFunction> for DFAggregateFunction {
    fn from(v: AggregateFunction) -> DFAggregateFunction {
        match v {
            AggregateFunction::Count => DFAggregateFunction::Count,
            AggregateFunction::Sum => DFAggregateFunction::Sum,
            AggregateFunction::Min => DFAggregateFunction::Min,
            AggregateFunction::Max => DFAggregateFunction::Max,
            AggregateFunction::Avg => DFAggregateFunction::Avg,
            AggregateFunction::Median => DFAggregateFunction::Median,
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
            AggregateFunction::ApproxPercentileContWithWeight => {
                DFAggregateFunction::ApproxPercentileContWithWeight
            }
            AggregateFunction::ApproxMedian => DFAggregateFunction::ApproxMedian,
            AggregateFunction::Grouping => DFAggregateFunction::Grouping,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum PartitionedAggregateFunction {
    Count,
    Sum,
}

impl fmt::Display for PartitionedAggregateFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", format!("{:?}", self).to_uppercase())
    }
}

/// Operators applied to expressions
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum Operator {
    /// Expressions are equal
    Eq,
    /// Expressions are not equal
    NotEq,
    /// Left side is smaller than right side
    Lt,
    /// Left side is smaller or equal to right side
    LtEq,
    /// Left side is greater than right side
    Gt,
    /// Left side is greater or equal to right side
    GtEq,
    /// Addition
    Plus,
    /// Subtraction
    Minus,
    /// Multiplication operator, like `*`
    Multiply,
    /// Division operator, like `/`
    Divide,
    /// Remainder operator, like `%`
    Modulo,
    /// Logical AND, like `&&`
    And,
    /// Logical OR, like `||`
    Or,
    /// Matches a wildcard pattern
    Like,
    /// Does not match a wildcard pattern
    NotLike,
    /// IS DISTINCT FROM
    IsDistinctFrom,
    /// IS NOT DISTINCT FROM
    IsNotDistinctFrom,
    /// Case sensitive regex match
    RegexMatch,
    /// Case insensitive regex match
    RegexIMatch,
    /// Case sensitive regex not match
    RegexNotMatch,
    /// Case insensitive regex not match
    RegexNotIMatch,
    /// Bitwise and, like `&`
    BitwiseAnd,
    /// Bitwise or, like `|`
    BitwiseOr,
    /// Bitwise xor, like `#`
    BitwiseXor,
    /// Bitwise right, like `>>`
    BitwiseShiftRight,
    /// Bitwise left, like `<<`
    BitwiseShiftLeft,
    /// String concat
    StringConcat,
}

impl From<DFOperator> for Operator {
    fn from(o: DFOperator) -> Self {
        match o {
            DFOperator::Eq => Operator::Eq,
            DFOperator::NotEq => Operator::NotEq,
            DFOperator::Lt => Operator::Lt,
            DFOperator::LtEq => Operator::LtEq,
            DFOperator::Gt => Operator::Gt,
            DFOperator::GtEq => Operator::GtEq,
            DFOperator::Plus => Operator::Plus,
            DFOperator::Minus => Operator::Minus,
            DFOperator::Multiply => Operator::Multiply,
            DFOperator::Divide => Operator::Divide,
            DFOperator::Modulo => Operator::Modulo,
            DFOperator::And => Operator::And,
            DFOperator::Or => Operator::Or,
            DFOperator::Like => Operator::Like,
            DFOperator::NotLike => Operator::NotLike,
            DFOperator::IsDistinctFrom => Operator::IsDistinctFrom,
            DFOperator::IsNotDistinctFrom => Operator::IsNotDistinctFrom,
            DFOperator::RegexMatch => Operator::RegexMatch,
            DFOperator::RegexIMatch => Operator::RegexIMatch,
            DFOperator::RegexNotMatch => Operator::RegexNotMatch,
            DFOperator::RegexNotIMatch => Operator::RegexNotIMatch,
            DFOperator::BitwiseAnd => Operator::BitwiseAnd,
            DFOperator::BitwiseOr => Operator::BitwiseOr,
            DFOperator::BitwiseXor => Operator::BitwiseXor,
            DFOperator::BitwiseShiftRight => Operator::BitwiseShiftRight,
            DFOperator::BitwiseShiftLeft => Operator::BitwiseShiftLeft,
            DFOperator::StringConcat => Operator::StringConcat,
        }
    }
}

impl From<Operator> for DFOperator {
    fn from(o: Operator) -> Self {
        match o {
            Operator::Eq => DFOperator::Eq,
            Operator::NotEq => DFOperator::NotEq,
            Operator::Lt => DFOperator::Lt,
            Operator::LtEq => DFOperator::LtEq,
            Operator::Gt => DFOperator::Gt,
            Operator::GtEq => DFOperator::GtEq,
            Operator::Plus => DFOperator::Plus,
            Operator::Minus => DFOperator::Minus,
            Operator::Multiply => DFOperator::Multiply,
            Operator::Divide => DFOperator::Divide,
            Operator::Modulo => DFOperator::Modulo,
            Operator::And => DFOperator::And,
            Operator::Or => DFOperator::Or,
            Operator::Like => DFOperator::Like,
            Operator::NotLike => DFOperator::NotLike,
            Operator::IsDistinctFrom => DFOperator::IsDistinctFrom,
            Operator::IsNotDistinctFrom => DFOperator::IsNotDistinctFrom,
            Operator::RegexMatch => DFOperator::RegexMatch,
            Operator::RegexIMatch => DFOperator::RegexIMatch,
            Operator::RegexNotMatch => DFOperator::RegexNotMatch,
            Operator::RegexNotIMatch => DFOperator::RegexNotIMatch,
            Operator::BitwiseAnd => DFOperator::BitwiseAnd,
            Operator::BitwiseOr => DFOperator::BitwiseOr,
            Operator::BitwiseXor => DFOperator::BitwiseXor,
            Operator::BitwiseShiftRight => DFOperator::BitwiseShiftRight,
            Operator::BitwiseShiftLeft => DFOperator::BitwiseShiftLeft,
            Operator::StringConcat => DFOperator::StringConcat,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum TimeUnit {
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}

impl TryInto<TimeUnit> for arrow_schema::TimeUnit {
    type Error = CommonError;

    fn try_into(self) -> std::result::Result<TimeUnit, Self::Error> {
        Ok(match self {
            arrow_schema::TimeUnit::Second => TimeUnit::Second,
            arrow_schema::TimeUnit::Millisecond => TimeUnit::Millisecond,
            arrow_schema::TimeUnit::Microsecond => TimeUnit::Microsecond,
            arrow_schema::TimeUnit::Nanosecond => TimeUnit::Nanosecond,
        })
    }
}

impl TryInto<arrow_schema::TimeUnit> for TimeUnit {
    type Error = CommonError;

    fn try_into(self) -> std::result::Result<arrow_schema::TimeUnit, Self::Error> {
        Ok(match self {
            TimeUnit::Second => arrow_schema::TimeUnit::Second,
            TimeUnit::Millisecond => arrow_schema::TimeUnit::Millisecond,
            TimeUnit::Microsecond => arrow_schema::TimeUnit::Microsecond,
            TimeUnit::Nanosecond => arrow_schema::TimeUnit::Nanosecond,
        })
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Hash, Eq, PartialEq)]
pub enum PropertyRef {
    User(String),
    Event(String),
    Custom(u64),
}

impl PropertyRef {
    pub fn name(&self) -> String {
        match self {
            PropertyRef::User(name) => name.clone(),
            PropertyRef::Event(name) => name.clone(),
            PropertyRef::Custom(_id) => unimplemented!(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum EventRef {
    RegularName(String),
    // TODO remove this, use only pk(id) addressing
    Regular(u64),
    Custom(u64),
}

impl EventRef {
    pub fn name(&self) -> String {
        match self {
            EventRef::RegularName(name) => name.to_owned(),
            EventRef::Regular(id) => id.to_string(),
            EventRef::Custom(id) => id.to_string(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum PropValueOperation {
    Eq,
    Neq,
    Gt,
    Gte,
    Lt,
    Lte,
    True,
    False,
    Exists,
    Empty,
    ArrAll,
    ArrAny,
    ArrNone,
    Like,
    NotLike,
    Regex,
    NotRegex,
}

impl From<PropValueOperation> for Operator {
    fn from(pv: PropValueOperation) -> Self {
        match pv {
            PropValueOperation::Eq => Operator::Eq,
            PropValueOperation::Neq => Operator::NotEq,
            PropValueOperation::Like => Operator::Like,
            _ => panic!("unreachable"),
        }
    }
}

impl TryInto<DFOperator> for PropValueOperation {
    type Error = CommonError;

    fn try_into(self) -> std::result::Result<DFOperator, Self::Error> {
        Ok(match self {
            PropValueOperation::Eq => DFOperator::Eq,
            PropValueOperation::Neq => DFOperator::NotEq,
            PropValueOperation::Like => DFOperator::Like,
            _ => unimplemented!(),
        })
    }
}

#[serde_with::serde_as]
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum EventFilter {
    Property {
        property: PropertyRef,
        operation: PropValueOperation,
        #[serde_as(as = "Option<Vec<ScalarValueRef>>")]
        value: Option<Vec<ScalarValue>>,
    },
}

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

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
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

pub fn time_columns(
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    granularity: &TimeIntervalUnit,
) -> Vec<String> {
    let from = date_trunc(granularity, from).unwrap();
    let to = date_trunc(granularity, to).unwrap();
    let rule = match granularity {
        TimeIntervalUnit::Second => DateRule::secondly(from),
        TimeIntervalUnit::Minute => DateRule::minutely(from),
        TimeIntervalUnit::Hour => DateRule::hourly(from),
        TimeIntervalUnit::Day => DateRule::daily(from),
        TimeIntervalUnit::Week => DateRule::weekly(from),
        TimeIntervalUnit::Month => DateRule::monthly(from),
        TimeIntervalUnit::Year => DateRule::yearly(from),
    };

    rule.with_end(to + granularity.relative_duration(1))
        .map(|dt| dt.naive_utc().to_string())
        .collect()
}

pub fn date_trunc(granularity: &TimeIntervalUnit, value: DateTime<Utc>) -> Result<DateTime<Utc>> {
    let value = Some(value);
    let value = match granularity {
        TimeIntervalUnit::Second => value,
        TimeIntervalUnit::Minute => value.and_then(|d| d.with_second(0)),
        TimeIntervalUnit::Hour => value
            .and_then(|d| d.with_second(0))
            .and_then(|d| d.with_minute(0)),
        TimeIntervalUnit::Day => value
            .and_then(|d| d.with_second(0))
            .and_then(|d| d.with_minute(0))
            .and_then(|d| d.with_hour(0)),
        TimeIntervalUnit::Week => value
            .and_then(|d| d.with_second(0))
            .and_then(|d| d.with_minute(0))
            .and_then(|d| d.with_hour(0))
            .map(|d| d - Duration::seconds(60 * 60 * 24 * d.weekday() as i64)),
        TimeIntervalUnit::Month => value
            .and_then(|d| d.with_second(0))
            .and_then(|d| d.with_minute(0))
            .and_then(|d| d.with_hour(0))
            .and_then(|d| d.with_day0(0)),
        TimeIntervalUnit::Year => value
            .and_then(|d| d.with_second(0))
            .and_then(|d| d.with_minute(0))
            .and_then(|d| d.with_hour(0))
            .and_then(|d| d.with_day0(0))
            .and_then(|d| d.with_month0(0)),
    };

    Ok(value.unwrap())
}
