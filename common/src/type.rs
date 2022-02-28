use arrow::datatypes::DataType as ArrowDataType;
use arrow::datatypes::TimeUnit::Second;
use rust_decimal::Decimal;
use datafusion::scalar::ScalarValue as DFScalarValue;

pub const DECIMAL_PRECISION: usize = 28;
pub const DECIMAL_SCALE: usize = 28;

pub enum DataType {
    Number,
    String,
    Boolean,
    Timestamp,
}

impl DataType {
    pub fn to_arrow(self) -> ArrowDataType {
        match self {
            DataType::Number => ArrowDataType::Decimal(DECIMAL_PRECISION, DECIMAL_SCALE),
            DataType::String => ArrowDataType::Utf8,
            DataType::Boolean => ArrowDataType::Boolean,
            DataType::Timestamp => ArrowDataType::Timestamp(Second, None)
        }
    }
}

pub enum ScalarValue {
    Number(Option<Decimal>),
    String(Option<String>),
    Boolean(Option<bool>),
    Timestamp(Option<i64>),
}

impl ScalarValue {
    pub fn to_df(self) -> DFScalarValue {
        match self {
            ScalarValue::Number(v) => {
                match v {
                    None => DFScalarValue::Decimal128(None, 0, 0),
                    Some(v) => DFScalarValue::Decimal128(Some(v.mantissa()), DECIMAL_PRECISION, v.scale() as usize),
                }
            }
            ScalarValue::String(v) => DFScalarValue::Utf8(v),
            ScalarValue::Boolean(v) => DFScalarValue::Boolean(v),
            ScalarValue::Timestamp(v) => DFScalarValue::TimestampSecond(v, None)
        }
    }
}