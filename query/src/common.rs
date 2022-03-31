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
        n: i64,
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
    pub fn sub(&self, n: i64) -> DateTime<Utc> {
        match self {
            TimeUnit::Second => Utc::now().sub(Duration::seconds(n)),
            TimeUnit::Minute => Utc::now().sub(Duration::minutes(n)),
            TimeUnit::Hour => Utc::now().sub(Duration::hours(n)),
            TimeUnit::Day => Utc::now().sub(Duration::days(n)),
            TimeUnit::Week => Utc::now().sub(Duration::weeks(n)),
            TimeUnit::Month => Utc::now().sub(Duration::days(n) * 30),
            TimeUnit::Year => Utc::now().sub(Duration::days(n) * 365),
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
