use arrow::datatypes::{DataType, Field};
use datafusion_common::ScalarValue as DFScalarValue;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum ScalarValue {
    /// represents `DataType::Null` (castable to/from any other type)
    Null,
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
    /// utf-8 encoded string representing a LargeString's arrow type.
    LargeUtf8(Option<String>),
    /// binary
    Binary(Option<Vec<u8>>),
    /// large binary
    LargeBinary(Option<Vec<u8>>),
    /// list of nested ScalarValue
    List(Option<Vec<ScalarValue>>, Box<Field>),
    /// Date stored as a signed 32bit int days since UNIX epoch 1970-01-01
    Date32(Option<i32>),
    /// Date stored as a signed 64bit int milliseconds since UNIX epoch 1970-01-01
    Date64(Option<i64>),
    /// Time stored as a signed 64bit int as nanoseconds since midnight
    Time64(Option<i64>),
    /// Timestamp Second
    TimestampSecond(Option<i64>, Option<String>),
    /// Timestamp Milliseconds
    TimestampMillisecond(Option<i64>, Option<String>),
    /// Timestamp Microseconds
    TimestampMicrosecond(Option<i64>, Option<String>),
    /// Timestamp Nanoseconds
    TimestampNanosecond(Option<i64>, Option<String>),
    /// Number of elapsed whole months
    IntervalYearMonth(Option<i32>),
    /// Number of elapsed days and milliseconds (no leap seconds)
    /// stored as 2 contiguous 32-bit signed integers
    IntervalDayTime(Option<i64>),
    /// A triple of the number of elapsed months, days, and nanoseconds.
    /// Months and days are encoded as 32-bit signed integers.
    /// Nanoseconds is encoded as a 64-bit signed integer (no leap seconds).
    IntervalMonthDayNano(Option<i128>),
    /// struct of nested ScalarValue
    Struct(Option<Vec<ScalarValue>>, Box<Vec<Field>>),
    /// Dictionary type: index type and value
    Dictionary(Box<DataType>, Box<ScalarValue>),
}

impl From<ScalarValue> for DFScalarValue {
    fn from(v: ScalarValue) -> Self {
        match v {
            ScalarValue::Null => DFScalarValue::Null,
            ScalarValue::Boolean(v) => DFScalarValue::Boolean(v),
            ScalarValue::Float32(v) => DFScalarValue::Float32(v),
            ScalarValue::Float64(v) => DFScalarValue::Float64(v),
            ScalarValue::Decimal128(v, p, s) => DFScalarValue::Decimal128(v, p, s),
            ScalarValue::Int8(v) => DFScalarValue::Int8(v),
            ScalarValue::Int16(v) => DFScalarValue::Int16(v),
            ScalarValue::Int32(v) => DFScalarValue::Int32(v),
            ScalarValue::Int64(v) => DFScalarValue::Int64(v),
            ScalarValue::UInt8(v) => DFScalarValue::UInt8(v),
            ScalarValue::UInt16(v) => DFScalarValue::UInt16(v),
            ScalarValue::UInt32(v) => DFScalarValue::UInt32(v),
            ScalarValue::UInt64(v) => DFScalarValue::UInt64(v),
            ScalarValue::Utf8(v) => DFScalarValue::Utf8(v),
            ScalarValue::LargeUtf8(v) => DFScalarValue::LargeUtf8(v),
            ScalarValue::Binary(v) => DFScalarValue::Binary(v),
            ScalarValue::LargeBinary(v) => DFScalarValue::LargeBinary(v),
            ScalarValue::List(v, f) => DFScalarValue::List(
                v.map(|v| {
                    v.iter()
                        .map(|v| v.to_owned().into())
                        .collect::<Vec<DFScalarValue>>()
                }),
                f,
            ),
            ScalarValue::Date32(v) => DFScalarValue::Date32(v),
            ScalarValue::Date64(v) => DFScalarValue::Date64(v),
            ScalarValue::Time64(v) => DFScalarValue::Time64(v),
            ScalarValue::TimestampSecond(v, f) => DFScalarValue::TimestampSecond(v, f),
            ScalarValue::TimestampMillisecond(v, f) => DFScalarValue::TimestampMillisecond(v, f),
            ScalarValue::TimestampMicrosecond(v, f) => DFScalarValue::TimestampMicrosecond(v, f),
            ScalarValue::TimestampNanosecond(v, f) => DFScalarValue::TimestampNanosecond(v, f),
            ScalarValue::IntervalYearMonth(v) => DFScalarValue::IntervalYearMonth(v),
            ScalarValue::IntervalDayTime(v) => DFScalarValue::IntervalDayTime(v),
            ScalarValue::IntervalMonthDayNano(v) => DFScalarValue::IntervalMonthDayNano(v),
            ScalarValue::Struct(v, f) => DFScalarValue::Struct(
                v.map(|v| {
                    v.iter()
                        .map(|v| v.to_owned().into())
                        .collect::<Vec<DFScalarValue>>()
                }),
                f,
            ),
            ScalarValue::Dictionary(t, v) => DFScalarValue::Dictionary(t, Box::new((*v).into())),
        }
    }
}

impl From<DFScalarValue> for ScalarValue {
    fn from(v: DFScalarValue) -> Self {
        match v {
            DFScalarValue::Null => ScalarValue::Null,
            DFScalarValue::Boolean(v) => ScalarValue::Boolean(v),
            DFScalarValue::Float32(v) => ScalarValue::Float32(v),
            DFScalarValue::Float64(v) => ScalarValue::Float64(v),
            DFScalarValue::Decimal128(v, p, s) => ScalarValue::Decimal128(v, p, s),
            DFScalarValue::Int8(v) => ScalarValue::Int8(v),
            DFScalarValue::Int16(v) => ScalarValue::Int16(v),
            DFScalarValue::Int32(v) => ScalarValue::Int32(v),
            DFScalarValue::Int64(v) => ScalarValue::Int64(v),
            DFScalarValue::UInt8(v) => ScalarValue::UInt8(v),
            DFScalarValue::UInt16(v) => ScalarValue::UInt16(v),
            DFScalarValue::UInt32(v) => ScalarValue::UInt32(v),
            DFScalarValue::UInt64(v) => ScalarValue::UInt64(v),
            DFScalarValue::Utf8(v) => ScalarValue::Utf8(v),
            DFScalarValue::LargeUtf8(v) => ScalarValue::LargeUtf8(v),
            DFScalarValue::Binary(v) => ScalarValue::Binary(v),
            DFScalarValue::LargeBinary(v) => ScalarValue::LargeBinary(v),
            DFScalarValue::List(v, f) => ScalarValue::List(
                v.map(|v| {
                    v.iter()
                        .map(|v| v.to_owned().into())
                        .collect::<Vec<ScalarValue>>()
                }),
                f,
            ),
            DFScalarValue::Date32(v) => ScalarValue::Date32(v),
            DFScalarValue::Date64(v) => ScalarValue::Date64(v),
            DFScalarValue::Time64(v) => ScalarValue::Time64(v),
            DFScalarValue::TimestampSecond(v, f) => ScalarValue::TimestampSecond(v, f),
            DFScalarValue::TimestampMillisecond(v, f) => ScalarValue::TimestampMillisecond(v, f),
            DFScalarValue::TimestampMicrosecond(v, f) => ScalarValue::TimestampMicrosecond(v, f),
            DFScalarValue::TimestampNanosecond(v, f) => ScalarValue::TimestampNanosecond(v, f),
            DFScalarValue::IntervalYearMonth(v) => ScalarValue::IntervalYearMonth(v),
            DFScalarValue::IntervalDayTime(v) => ScalarValue::IntervalDayTime(v),
            DFScalarValue::IntervalMonthDayNano(v) => ScalarValue::IntervalMonthDayNano(v),
            DFScalarValue::Struct(v, f) => ScalarValue::Struct(
                v.map(|v| {
                    v.iter()
                        .map(|v| v.to_owned().into())
                        .collect::<Vec<ScalarValue>>()
                }),
                f,
            ),
            DFScalarValue::Dictionary(t, v) => ScalarValue::Dictionary(t, Box::new((*v).into())),
        }
    }
}
