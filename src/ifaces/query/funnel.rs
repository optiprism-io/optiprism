use super::Operator;
use datafusion::scalar::ScalarValue;
use chrono::{Date, Utc, Duration};
use crate::ifaces::query::{PropertyOpValue, TimeRange, Property};
use super::Event;
use super::user_segment::UserSegment;

pub enum Filter {
    DropOffOnAnyStep,
    DropOffOnStep(usize),
    TimeToConvert(Duration, Duration),
}

enum Group {
    Property(Property),
    UserSegment(UserSegment),
}

enum Query {
    Steps,
    ConversionOverTime,
    TimeToConvert {},
    Frequency {},
}

enum Count {
    Uniques,
    Totals,
}

pub struct FunnelRequest {
    count: Count,
    query: Query,
    group_by: Group,
    time_range: TimeRange,
    window: Duration,
    steps: Vec<Event>,
    exclude: Option<Vec<(Event, Vec<usize>)>>,
    user_constant_properties: Option<Vec<String>>,
    filter: Option<Filter>,
}