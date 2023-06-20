use std::fmt::Debug;
use std::fmt::Formatter;
use std::marker::PhantomData;

use chrono::DateTime;
use chrono::Duration;
use chrono::Utc;
use common::query::TimeUnit;
use dyn_clone::DynClone;

#[derive(Debug)]
enum TimeRangeName {
    Between,
    From,
    Last,
    None,
}

pub trait TimeRange: Send + Sync + Debug + DynClone {
    fn check_bounds(&self, v: i64) -> bool;
    fn fn_name(&self) -> TimeRangeName;
}

#[derive(Clone)]
pub struct Between {
    pub from: i64,
    pub to: i64,
}

impl Between {
    pub fn new(from: DateTime<Utc>, to: DateTime<Utc>) -> Self {
        Self {
            from: from.timestamp_millis(),
            to: to.timestamp_millis(),
        }
    }
}

impl TimeRange for Between {
    fn check_bounds(&self, v: i64) -> bool {
        v >= self.from && v <= self.to
    }

    fn fn_name(&self) -> TimeRangeName {
        TimeRangeName::Between
    }
}

impl Debug for Between {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", TimeRangeName::Between)
    }
}

#[derive(Clone)]
pub struct From {
    pub from: i64,
}

impl From {
    pub fn new(from: DateTime<Utc>) -> Self {
        Self {
            from: from.timestamp_millis(),
        }
    }
}

impl TimeRange for From {
    fn check_bounds(&self, v: i64) -> bool {
        v >= self.from
    }

    fn fn_name(&self) -> TimeRangeName {
        TimeRangeName::From
    }
}

impl Debug for From {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", TimeRangeName::From)
    }
}

#[derive(Clone)]
pub struct Last {
    pub since: i64,
    pub ts: i64,
}

impl Last {
    pub fn new(since: Duration, ts: DateTime<Utc>) -> Self {
        Self {
            since: since.num_milliseconds(),
            ts: ts.timestamp_millis(),
        }
    }
}

impl TimeRange for Last {
    fn check_bounds(&self, v: i64) -> bool {
        v >= self.ts - self.since
    }

    fn fn_name(&self) -> TimeRangeName {
        TimeRangeName::Last
    }
}

impl Debug for Last {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", TimeRangeName::Last)
    }
}

#[derive(Clone)]
pub struct TimeRangeNone {}

impl TimeRangeNone {
    pub fn new() -> Self {
        Self {}
    }
}

impl TimeRange for TimeRangeNone {
    fn check_bounds(&self, v: i64) -> bool {
        true
    }

    fn fn_name(&self) -> TimeRangeName {
        TimeRangeName::None
    }
}

impl Debug for TimeRangeNone {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", TimeRangeName::None)
    }
}
