use chrono::{DateTime, Utc};
use chronoutil::RelativeDuration;
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
            QueryTime::Last { last, unit } => (cur_time + unit.relative_duration(-*last), cur_time),
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
