pub mod event_segmentation;

use chrono::DateTime;
use chrono::Utc;
use chronoutil::RelativeDuration;

#[derive(Clone, Debug)]
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

#[derive(Clone, Debug)]
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
