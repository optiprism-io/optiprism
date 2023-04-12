use ordered_float::OrderedFloat;
use arrow2::datatypes::{DataType, TimeUnit};
use parquet2::schema::types::PhysicalType;

pub mod parquet;
pub mod arrow;
pub mod merger;

#[derive(Eq, PartialEq, PartialOrd, Ord, Debug, Clone)]
pub enum Value {
    Boolean(bool),
    Int32(i32),
    Int64(i64),
    Int96([u32; 3]),
    Float(OrderedFloat<f32>),
    Double(OrderedFloat<f64>),
    ByteArray(Vec<u8>),
}

impl From<bool> for Value {
    fn from(value: bool) -> Self {
        Value::Boolean(value)
    }
}

impl From<i32> for Value {
    fn from(value: i32) -> Self {
        Value::Int32(value)
    }
}

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Value::Int64(value)
    }
}

impl From<[u32; 3]> for Value {
    fn from(value: [u32; 3]) -> Self {
        Value::Int96(value)
    }
}

impl From<f32> for Value {
    fn from(value: f32) -> Self {
        Value::Float(OrderedFloat(value))
    }
}

impl From<f64> for Value {
    fn from(value: f64) -> Self {
        Value::Double(OrderedFloat(value))
    }
}

impl From<Vec<u8>> for Value {
    fn from(value: Vec<u8>) -> Self {
        Value::ByteArray(value)
    }
}

pub type ReorderSlice = (usize, usize, usize);

pub fn from_physical_type(t: &PhysicalType) -> DataType {
    match t {
        PhysicalType::Boolean => DataType::Boolean,
        PhysicalType::Int32 => DataType::Int32,
        PhysicalType::Int64 => DataType::Int64,
        PhysicalType::Float => DataType::Float32,
        PhysicalType::Double => DataType::Float64,
        PhysicalType::ByteArray => DataType::Utf8,
        PhysicalType::FixedLenByteArray(l) => DataType::FixedSizeBinary(*l),
        PhysicalType::Int96 => DataType::Timestamp(TimeUnit::Nanosecond, None),
    }
}