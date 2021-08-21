use datafusion::scalar::ScalarValue;
use chrono::{Date, Utc};

mod event_segmentation;
mod segment;

use arrow::datatypes::DataType;
use crate::ifaces::query::event_segmentation::EventSegmentation;
use datafusion::error::Result;

trait Query {
    fn event_segmentation(&self, query: &EventSegmentation) -> Result<QueryResult>;
}

pub enum Operator {
    Eq,
    Neq,
    Gt,
    Gte,
    Lt,
    Lte,
}

pub enum PropertyValue {
    Exact(ScalarValue),
    OneOf(Vec<ScalarValue>),
}

pub enum PropertyScope {
    User,
    Event,
}

pub struct PropertyOpValue {
    scope: PropertyScope,
    name: String,
    op: Operator,
    value: PropertyValue,
}

pub enum Property {
    User(String),
    // string - имя свойства
    Event(String),
}

pub struct Event {
    name: String,
    properties: Vec<PropertyOpValue>,
}

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

pub enum Order {
    Property(Property),
    Expr(Expr),
}

pub enum OrderDirection {
    Asc,
    Desc,
}

pub enum TimeBucket {
    Hourly,
    Daily,
    Weekly,
    Monthly,
    Quarterly,
}

pub enum TimeRange {
    LastDays(usize),
    Since(Date<Utc>),
    Between(Date<Utc>, Date<Utc>),
}

enum ColumnType {
    Dimension,
    Metric,
}

pub struct Column {
    name: String,
    typ: ColumnType,
    data_type: DataType,
    is_nullable: bool,
    data: Vec<ScalarValue>,
}

pub struct QueryResult {
    columns: Vec<Column>,
    summary: Vec<ScalarValue>,
}