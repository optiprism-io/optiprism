mod event_segmentation;
mod funnel;
mod user_segment;

use super::dquery::EventSegmentation;
use arrow::datatypes::DataType;
use chrono::{Date, Utc};
use datafusion::error::Result;
use datafusion::scalar::ScalarValue;

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

pub enum Property {
    User {
        property_name: String,
    },
    Event {
        event_name: String,
        property_name: String,
    },
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

pub struct Event {
    name: String,
    properties: Vec<PropertyOpValue>,
}

pub enum TimeGroup {
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
