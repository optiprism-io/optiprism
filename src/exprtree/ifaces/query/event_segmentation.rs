use datafusion::scalar::ScalarValue;
use chrono::{Date, Utc};
use super::{Operator, Event, TimeRange, TimeGroup};
use super::user_segment::UserSegment;
use crate::exprtree::ifaces::query::Property;


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

enum Group {
    Property(Property),
    UserSegment(UserSegment),
}

pub enum Order {
    Property(Property),
    Expr(Expr),
}

pub enum OrderDirection {
    Asc,
    Desc,
}

pub struct EventSegmentationRequest {
    events: Vec<Event>,
    group_by: Vec<Group>,
    aggregate_by: Vec<Expr>,
    order_by: Option<Vec<(Order, OrderDirection)>>,
    user_segments: Option<Vec<UserSegment>>,
    time_range: TimeRange,
    group_by_time: TimeGroup,
}
