use arrow::datatypes::DataType as ArrowDataType;
use arrow::datatypes::TimeUnit;
use common::DECIMAL_PRECISION;
use common::DECIMAL_SCALE;

use serde::Deserialize;

use serde::Serialize;


use crate::PlatformError;

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum DataType {
    Number,
    String,
    Boolean,
    Timestamp(TimeUnit),
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    LargeUtf8,
}

impl TryInto<ArrowDataType> for DataType {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<ArrowDataType, Self::Error> {
        Ok(match self {
            DataType::Number => ArrowDataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE),
            DataType::String => ArrowDataType::Utf8,
            DataType::LargeUtf8 => ArrowDataType::LargeUtf8,
            DataType::Boolean => ArrowDataType::Boolean,
            DataType::Timestamp(tu) => ArrowDataType::Timestamp(tu, None),
            DataType::Int8 => ArrowDataType::Int8,
            DataType::Int16 => ArrowDataType::Int16,
            DataType::Int32 => ArrowDataType::Int32,
            DataType::Int64 => ArrowDataType::Int64,
            DataType::UInt8 => ArrowDataType::UInt8,
            DataType::UInt16 => ArrowDataType::UInt16,
            DataType::UInt32 => ArrowDataType::UInt32,
            DataType::UInt64 => ArrowDataType::UInt64,
            DataType::Float32 => ArrowDataType::Float32,
            DataType::Float64 => ArrowDataType::Float64,
        })
    }
}

impl TryInto<DataType> for ArrowDataType {
    type Error = PlatformError;

    fn try_into(self) -> Result<DataType, Self::Error> {
        Ok(match self {
            ArrowDataType::Boolean => DataType::Boolean,
            ArrowDataType::Utf8 => DataType::String,
            ArrowDataType::LargeUtf8 => DataType::LargeUtf8,
            ArrowDataType::Timestamp(tu, _) => DataType::Timestamp(tu),
            ArrowDataType::Int8 => DataType::Int8,
            ArrowDataType::Int16 => DataType::Int16,
            ArrowDataType::Int32 => DataType::Int32,
            ArrowDataType::Int64 => DataType::Int64,
            ArrowDataType::UInt8 => DataType::UInt8,
            ArrowDataType::UInt16 => DataType::UInt16,
            ArrowDataType::UInt32 => DataType::UInt32,
            ArrowDataType::UInt64 => DataType::UInt64,
            ArrowDataType::Float32 => DataType::Float32,
            ArrowDataType::Float64 => DataType::Float64,
            ArrowDataType::Decimal128(_, _) => DataType::Number,
            _ => return Err(PlatformError::EntityMap),
        })
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum DictionaryDataType {
    #[serde(rename = "uint8")]
    UInt8,
    #[serde(rename = "uint16")]
    UInt16,
    #[serde(rename = "uint32")]
    UInt32,
    #[serde(rename = "uint64")]
    UInt64,
}

impl TryInto<ArrowDataType> for DictionaryDataType {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<ArrowDataType, Self::Error> {
        Ok(match self {
            DictionaryDataType::UInt8 => ArrowDataType::UInt8,
            DictionaryDataType::UInt16 => ArrowDataType::UInt16,
            DictionaryDataType::UInt32 => ArrowDataType::UInt32,
            DictionaryDataType::UInt64 => ArrowDataType::UInt64,
        })
    }
}

impl TryInto<DictionaryDataType> for ArrowDataType {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<DictionaryDataType, Self::Error> {
        Ok(match self {
            ArrowDataType::UInt8 => DictionaryDataType::UInt8,
            ArrowDataType::UInt16 => DictionaryDataType::UInt16,
            ArrowDataType::UInt32 => DictionaryDataType::UInt32,
            ArrowDataType::UInt64 => DictionaryDataType::UInt64,
            _ => return Err(PlatformError::EntityMap),
        })
    }
}
