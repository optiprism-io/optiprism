use datafusion::scalar::ScalarValue;
use chrono::{Date, Utc};
use super::{Operator, Event, Order, OrderDirection, TimeRange, TimeBucket};
use super::segment::Segment;


pub enum Expr {
    Count(Property),
    DistinctCount(Property),
    Sum(Property),
    Avg(Property),
    Mul {
        left: Box<Expr>,
        right: Box<Expr>,
    },
    Div {
        left: Box<Expr>,
        right: Box<Expr>,
    },
    Lit(ScalarValue),
}


pub enum Property {
    User {
        property_name: String,
    },
    Event {
        event_name: String,
        property_name: String,
    },
}

enum Group {
    EventName(String),
    Property {
        event_name: String,
        property: Property,
    },
}

pub struct EventSegmentation {
    events: Vec<Event>,
    group_by: Vec<Group>,
    aggregate_by: Vec<Expr>,
    order_by: Vec<(Order, OrderDirection)>,
    segments: Vec<Segment>,
    time_range: TimeRange,
    group_by_time: TimeBucket,
}

