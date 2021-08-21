use datafusion::scalar::ScalarValue;
use chrono::{Date, Utc};
use super::{Operator, Event, Property, Expr, Order, OrderDirection, TimeRange, TimeBucket};
use super::segment::Segment;


pub struct EventSegmentation {
    events: Vec<Event>,
    group_by: Vec<Property>,
    aggregate_by: Vec<Expr>,
    order_by: Vec<(Order, OrderDirection)>,
    segments: Vec<Segment>,
    time_range: TimeRange,
    group_by_time: TimeBucket,
}

