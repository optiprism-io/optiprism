use chrono::{DateTime, Duration, Utc};
use crate::error::QueryError::Plan;

struct FirstBucket {
    converted: usize,
}

struct NextBucket {
    converted: usize,
    dropped_off: usize,
    average_time: Duration,
    average_time_from_start: Duration,
    conversion_ratio: f64,
    drop_off_ratio: f64,
    step_conversion_ratio: f64,
}

enum Bucket {
    First(FirstBucket),
    Next(NextBucket),
}

struct Step {
    buckets: Vec<Bucket>,
}

struct Row {
    ts: DateTime<Utc>,
    pk: usize,
    metrics: Vec<Step>,
}

struct Steps {
    rows: Vec<Row>,
}

struct TSRow {
    ts: i64,
    pk: usize,
    metrics: Vec<Step>,
}

struct Trends {
    rows: Vec<TSRow>,
}

struct TimeToConvert {
    rows: Vec<TSRow>,
}


struct Path {
    from: Bucket,
    to: Bucket,
}

struct TopPaths {
    paths: Vec<Path>,
}