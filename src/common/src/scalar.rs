use std::sync::Arc;

use arrow_schema::DataType;
use datafusion_common::ScalarValue;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;
use serde_with::DeserializeAs;
use serde_with::SerializeAs;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum ScalarValueRef {
    Null,
    Boolean(Option<bool>),
    Float32(Option<f32>),
    Float64(Option<f64>),
    Decimal128(Option<i128>, u8, i8),
    Int8(Option<i8>),
    Int16(Option<i16>),
    Int32(Option<i32>),
    Int64(Option<i64>),
    UInt8(Option<u8>),
    UInt16(Option<u16>),
    UInt32(Option<u32>),
    UInt64(Option<u64>),
    Utf8(Option<String>),
    LargeUtf8(Option<String>),
    Binary(Option<Vec<u8>>),
    FixedSizeBinary(i32, Option<Vec<u8>>),
    LargeBinary(Option<Vec<u8>>),
    Date32(Option<i32>),
    Date64(Option<i64>),
    Time32Second(Option<i32>),
    Time32Millisecond(Option<i32>),
    Time64Microsecond(Option<i64>),
    Time64Nanosecond(Option<i64>),
    TimestampSecond(Option<i64>, Option<Arc<str>>),
    TimestampMillisecond(Option<i64>, Option<Arc<str>>),
    TimestampMicrosecond(Option<i64>, Option<Arc<str>>),
    TimestampNanosecond(Option<i64>, Option<Arc<str>>),
    IntervalYearMonth(Option<i32>),
    IntervalDayTime(Option<i64>),
    IntervalMonthDayNano(Option<i128>),
    Dictionary(Box<DataType>, Box<ScalarValueRef>),
}

impl From<ScalarValueRef> for ScalarValue {
    fn from(v: ScalarValueRef) -> Self {
        match v {
            ScalarValueRef::Null => ScalarValue::Null,
            ScalarValueRef::Boolean(v) => ScalarValue::Boolean(v),
            ScalarValueRef::Float32(v) => ScalarValue::Float32(v),
            ScalarValueRef::Float64(v) => ScalarValue::Float64(v),
            ScalarValueRef::Decimal128(v, p, s) => ScalarValue::Decimal128(v, p, s),
            ScalarValueRef::Int8(v) => ScalarValue::Int8(v),
            ScalarValueRef::Int16(v) => ScalarValue::Int16(v),
            ScalarValueRef::Int32(v) => ScalarValue::Int32(v),
            ScalarValueRef::Int64(v) => ScalarValue::Int64(v),
            ScalarValueRef::UInt8(v) => ScalarValue::UInt8(v),
            ScalarValueRef::UInt16(v) => ScalarValue::UInt16(v),
            ScalarValueRef::UInt32(v) => ScalarValue::UInt32(v),
            ScalarValueRef::UInt64(v) => ScalarValue::UInt64(v),
            ScalarValueRef::Utf8(v) => ScalarValue::Utf8(v),
            ScalarValueRef::LargeUtf8(v) => ScalarValue::LargeUtf8(v),
            ScalarValueRef::Binary(v) => ScalarValue::Binary(v),
            ScalarValueRef::LargeBinary(v) => ScalarValue::LargeBinary(v),
            ScalarValueRef::Date32(v) => ScalarValue::Date32(v),
            ScalarValueRef::Date64(v) => ScalarValue::Date64(v),
            ScalarValueRef::TimestampSecond(v, f) => ScalarValue::TimestampSecond(v, f),
            ScalarValueRef::TimestampMillisecond(v, f) => ScalarValue::TimestampMillisecond(v, f),
            ScalarValueRef::TimestampMicrosecond(v, f) => ScalarValue::TimestampMicrosecond(v, f),
            ScalarValueRef::TimestampNanosecond(v, f) => ScalarValue::TimestampNanosecond(v, f),
            ScalarValueRef::IntervalYearMonth(v) => ScalarValue::IntervalYearMonth(v),
            ScalarValueRef::FixedSizeBinary(a, b) => ScalarValue::FixedSizeBinary(a, b),
            ScalarValueRef::Time32Second(v) => ScalarValue::Time32Second(v),
            ScalarValueRef::Time32Millisecond(v) => ScalarValue::Time32Second(v),
            ScalarValueRef::Time64Microsecond(v) => ScalarValue::Time64Microsecond(v),
            ScalarValueRef::Time64Nanosecond(v) => ScalarValue::Time64Nanosecond(v),
            _ => unimplemented!(),
        }
    }
}

impl From<&ScalarValue> for ScalarValueRef {
    fn from(v: &ScalarValue) -> Self {
        match v.clone() {
            ScalarValue::Null => ScalarValueRef::Null,
            ScalarValue::Boolean(v) => ScalarValueRef::Boolean(v),
            ScalarValue::Float32(v) => ScalarValueRef::Float32(v),
            ScalarValue::Float64(v) => ScalarValueRef::Float64(v),
            ScalarValue::Decimal128(v, p, s) => ScalarValueRef::Decimal128(v, p, s),
            ScalarValue::Int8(v) => ScalarValueRef::Int8(v),
            ScalarValue::Int16(v) => ScalarValueRef::Int16(v),
            ScalarValue::Int32(v) => ScalarValueRef::Int32(v),
            ScalarValue::Int64(v) => ScalarValueRef::Int64(v),
            ScalarValue::UInt8(v) => ScalarValueRef::UInt8(v),
            ScalarValue::UInt16(v) => ScalarValueRef::UInt16(v),
            ScalarValue::UInt32(v) => ScalarValueRef::UInt32(v),
            ScalarValue::UInt64(v) => ScalarValueRef::UInt64(v),
            ScalarValue::Utf8(v) => ScalarValueRef::Utf8(v),
            ScalarValue::LargeUtf8(v) => ScalarValueRef::LargeUtf8(v),
            ScalarValue::Binary(v) => ScalarValueRef::Binary(v),
            ScalarValue::LargeBinary(v) => ScalarValueRef::LargeBinary(v),
            ScalarValue::Date32(v) => ScalarValueRef::Date32(v),
            ScalarValue::Date64(v) => ScalarValueRef::Date64(v),
            ScalarValue::TimestampSecond(v, f) => ScalarValueRef::TimestampSecond(v, f),
            ScalarValue::TimestampMillisecond(v, f) => ScalarValueRef::TimestampMillisecond(v, f),
            ScalarValue::TimestampMicrosecond(v, f) => ScalarValueRef::TimestampMicrosecond(v, f),
            ScalarValue::TimestampNanosecond(v, f) => ScalarValueRef::TimestampNanosecond(v, f),
            ScalarValue::IntervalYearMonth(v) => ScalarValueRef::IntervalYearMonth(v),
            ScalarValue::FixedSizeBinary(a, b) => ScalarValueRef::FixedSizeBinary(a, b),
            ScalarValue::Time32Second(v) => ScalarValueRef::Time32Second(v),
            ScalarValue::Time32Millisecond(v) => ScalarValueRef::Time32Second(v),
            ScalarValue::Time64Microsecond(v) => ScalarValueRef::Time64Microsecond(v),
            ScalarValue::Time64Nanosecond(v) => ScalarValueRef::Time64Nanosecond(v),
            _ => unimplemented!(),
        }
    }
}

impl SerializeAs<ScalarValue> for ScalarValueRef {
    fn serialize_as<S>(value: &ScalarValue, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        ScalarValueRef::serialize(&ScalarValueRef::from(value), serializer)
    }
}

impl<'de> DeserializeAs<'de, ScalarValue> for ScalarValueRef {
    fn deserialize_as<D>(deserializer: D) -> Result<ScalarValue, D::Error>
    where D: Deserializer<'de> {
        Ok(ScalarValueRef::deserialize(deserializer)?.into())
    }
}
