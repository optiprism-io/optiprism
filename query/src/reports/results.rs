use arrow::record_batch::RecordBatch;
use datafusion_common::ScalarValue;
use crate::Result;

use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
pub struct Series {
    dimension_headers: Vec<String>,
    metric_headers: Vec<String>,
    dimensions: Vec<Value>,
    series: Vec<Value>,
}

#[derive(Serialize, Deserialize)]
enum Value {
    /// true or false value
    Boolean(Option<bool>),
    /// 32bit float
    Float32(Option<f32>),
    /// 64bit float
    Float64(Option<f64>),
    /// 128bit decimal, using the i128 to represent the decimal
    Decimal128(Option<i128>, usize, usize),
    /// signed 8bit int
    Int8(Option<i8>),
    /// signed 16bit int
    Int16(Option<i16>),
    /// signed 32bit int
    Int32(Option<i32>),
    /// signed 64bit int
    Int64(Option<i64>),
    /// unsigned 8bit int
    UInt8(Option<u8>),
    /// unsigned 16bit int
    UInt16(Option<u16>),
    /// unsigned 32bit int
    UInt32(Option<u32>),
    /// unsigned 64bit int
    UInt64(Option<u64>),
    /// utf-8 encoded string.
    Utf8(Option<String>),
    /// Timestamp Second
    TimestampSecond(Option<i64>, Option<String>),
    /// Timestamp Milliseconds
    TimestampMillisecond(Option<i64>, Option<String>),
    /// Timestamp Microseconds
    TimestampMicrosecond(Option<i64>, Option<String>),
    /// Timestamp Nanoseconds
    TimestampNanosecond(Option<i64>, Option<String>),
}

impl From<ScalarValue> for Value {
    fn from(v: ScalarValue) -> Self {
        match v {
            ScalarValue::Boolean(v) => Value::Boolean(v),
            ScalarValue::Float32(v) => Value::Float32(v),
            ScalarValue::Float64(v) => Value::Float64(v),
            ScalarValue::Decimal128(v, p, s) => Value::Decimal128(v, p, s),
            ScalarValue::Int8(v) => Value::Int8(v),
            ScalarValue::Int16(v) => Value::Int16(v),
            ScalarValue::Int32(v) => Value::Int32(v),
            ScalarValue::Int64(v) => Value::Int64(v),
            ScalarValue::UInt8(v) => Value::UInt8(v),
            ScalarValue::UInt16(v) => Value::UInt16(v),
            ScalarValue::UInt32(v) => Value::UInt32(v),
            ScalarValue::UInt64(v) => Value::UInt64(v),
            ScalarValue::Utf8(v) => Value::Utf8(v),
            ScalarValue::TimestampSecond(v, tz) => Value::TimestampSecond(v, tz),
            ScalarValue::TimestampMillisecond(v, tz) => Value::TimestampMillisecond(v, tz),
            ScalarValue::TimestampMicrosecond(v, tz) => Value::TimestampMicrosecond(v, tz),
            ScalarValue::TimestampNanosecond(v, tz) => Value::TimestampNanosecond(v, tz),
            _ => unimplemented!(),
        }
    }
}

impl Series {
    pub fn try_from_batch_record(batch: &RecordBatch, dimension_headers: Vec<String>, metric_headers: Vec<String>) -> Result<Self> {
        let mut dimensions: Vec<Value> = vec![];
        for header in dimension_headers.iter() {
            let arr = batch.column(batch.schema().index_of(header)?);
            for idx in 0..arr.len() {
                dimensions.push(ScalarValue::try_from_array(arr, idx)?.into());
            }
        }

        let mut series: Vec<Value> = vec![];
        for header in metric_headers.iter() {
            let arr = batch.column(batch.schema().index_of(header)?);
            for idx in 0..arr.len() {
                series.push(ScalarValue::try_from_array(arr, idx)?.into());
            }
        }

        Ok(Series {
            dimension_headers,
            metric_headers,
            dimensions,
            series,
        })
    }
}