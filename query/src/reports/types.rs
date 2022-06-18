use crate::physical_plan::expressions::partitioned_aggregate;
use chrono::{DateTime, Duration, Utc};
use chronoutil::RelativeDuration;
use datafusion::physical_plan::aggregates::AggregateFunction as DFAggregateFunction;
use datafusion_expr::Operator;

#[derive(Clone, Debug)]
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

impl QueryTime {
    pub fn range(&self, cur_time: DateTime<Utc>) -> (DateTime<Utc>, DateTime<Utc>) {
        match self {
            QueryTime::Between { from, to } => (*from, *to),
            QueryTime::From(from) => (*from, cur_time),
            QueryTime::Last { last, unit } => (cur_time + unit.relative_duration(*last), cur_time),
        }
    }
}

#[derive(Clone, Debug)]
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
    pub fn relative_duration(&self, n: i64) -> RelativeDuration {
        match self {
            TimeUnit::Second => RelativeDuration::seconds(n),
            TimeUnit::Minute => RelativeDuration::minutes(n),
            TimeUnit::Hour => RelativeDuration::hours(n),
            TimeUnit::Day => RelativeDuration::days(n),
            TimeUnit::Week => RelativeDuration::weeks(n),
            TimeUnit::Month => RelativeDuration::months(n as i32),
            TimeUnit::Year => RelativeDuration::years(n as i32),
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            TimeUnit::Second => "second",
            TimeUnit::Minute => "minute",
            TimeUnit::Hour => "hour",
            TimeUnit::Day => "day",
            TimeUnit::Week => "week",
            TimeUnit::Month => "month",
            TimeUnit::Year => "year",
        }
    }
}

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub enum PropertyRef {
    User(String),
    Event(String),
    Custom(String),
}

impl PropertyRef {
    pub fn name(&self) -> String {
        match self {
            PropertyRef::User(name) => name.clone(),
            PropertyRef::Event(name) => name.clone(),
            PropertyRef::Custom(name) => name.clone(),
        }
    }
}

#[derive(Clone, Debug)]
pub enum EventRef {
    Regular(String),
    Custom(String),
}

impl EventRef {
    pub fn name(&self) -> &str {
        match self {
            EventRef::Regular(name) => name.as_str(),
            EventRef::Custom(name) => name.as_str(),
        }
    }
}

#[derive(Clone, Debug)]
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
    Regex,
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
