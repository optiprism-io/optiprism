use chrono::prelude::*;
use chrono::DateTime;
use chrono::Duration;
use chrono::Utc;
use chronoutil::DateRule;

// use tokio::io::Error;
use crate::error::Result;
use crate::queries::event_segmentation::EventSegmentation;
use crate::queries::TimeIntervalUnit;

impl EventSegmentation {
    pub fn time_columns(&self, cur_time: DateTime<Utc>) -> Vec<String> {
        let (from, to) = self.time.range(cur_time);

        time_columns(from, to, &self.interval_unit)
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
