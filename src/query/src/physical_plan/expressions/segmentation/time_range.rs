use std::fmt::Debug;



use chrono::DateTime;
use chrono::Duration;
use chrono::Utc;



#[derive(Debug, Clone)]
pub enum TimeRange {
    Between(i64, i64),
    From(i64),
    Last(i64, i64),
    None,
}

pub fn from_milli(m: i64) -> DateTime<Utc> {
    DateTime::<Utc>::from_utc(
        chrono::NaiveDateTime::from_timestamp_millis(m).unwrap(),
        Utc,
    )
}

impl TimeRange {
    pub fn new_between(from: DateTime<Utc>, to: DateTime<Utc>) -> Self {
        TimeRange::Between(from.timestamp_millis(), to.timestamp_millis())
    }
    pub fn new_between_milli(from: i64, to: i64) -> Self {
        TimeRange::Between(from, to)
    }

    pub fn new_from(from: DateTime<Utc>) -> Self {
        TimeRange::From(from.timestamp_millis())
    }

    pub fn new_from_milli(from: i64) -> Self {
        TimeRange::From(from)
    }

    pub fn new_last(since: Duration, ts: DateTime<Utc>) -> Self {
        TimeRange::Last(since.num_milliseconds(), ts.timestamp_millis())
    }

    pub fn new_last_milli(since: i64, ts: i64) -> Self {
        TimeRange::Last(since, ts)
    }

    pub fn check_bounds(&self, ts: i64) -> bool {
        match self {
            TimeRange::Between(from, to) => ts >= *from && ts <= *to,
            TimeRange::From(from) => ts >= *from,
            TimeRange::Last(since, start_ts) => ts >= *start_ts - *since,
            TimeRange::None => true,
        }
    }
}
