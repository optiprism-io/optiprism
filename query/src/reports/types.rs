use chrono::{DateTime, Duration, Utc};
use datafusion_expr::Operator;
use datafusion::physical_plan::aggregates::AggregateFunction as DFAggregateFunction;
use crate::physical_plan::expressions::partitioned_aggregate;

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

#[derive(Clone, Debug)]
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
    pub fn name(&self) -> String {
        match self {
            EventRef::Regular(name) => name.clone(),
            EventRef::Custom(name) => name.clone(),
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