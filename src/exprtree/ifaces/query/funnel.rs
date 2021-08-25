use super::user_segment::UserSegment;
use super::Event;
use super::Operator;
use crate::exprtree::ifaces::query::{Property, PropertyOpValue, TimeRange};
use chrono::{Date, Duration, Utc};
use datafusion::scalar::ScalarValue;

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

pub struct FunnelRequest {
    query: Query,
    group_by: Group,
    time_range: TimeRange,
    window: Duration,
    steps: Vec<Event>,
    exclude: Option<Vec<(Event, Vec<usize>)>>,
    user_constant_properties: Option<Vec<String>>,
    filter: Option<Filter>,
}
