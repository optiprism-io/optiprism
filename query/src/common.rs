use chrono::{DateTime, Duration, Utc};
use datafusion_expr::Operator;
use std::ops::Sub;

#[derive(Clone)]
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

#[derive(Clone)]
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

#[derive(Clone)]
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

#[derive(Clone)]
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
