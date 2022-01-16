use super::user_segment::UserSegment;
use super::{Event, Operator, TimeGroup, TimeRange};
use crate::ifaces::query::Property;
use chrono::{Date, Utc};
use datafusion::scalar::ScalarValue;

pub enum Expr {
    Count(Property),
    DistinctCount(Property),
    Sum(Property),
    Avg(Property),
    Mul { left: Box<Expr>, right: Box<Expr> },
    Div { left: Box<Expr>, right: Box<Expr> },
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
