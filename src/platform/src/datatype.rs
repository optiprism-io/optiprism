use arrow::datatypes::DataType as ArrowDataType;
use arrow::datatypes::TimeUnit;
use common::types::TIME_UNIT;
use common::DECIMAL_PRECISION;
use common::DECIMAL_SCALE;
use metadata::properties;
use serde::Deserialize;
use serde::Serialize;

use crate::PlatformError;

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum DataType {
    Decimal,
    String,
    Boolean,
    Timestamp,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float64,
}

impl From<DataType> for properties::DataType {
    fn from(value: DataType) -> Self {
        match value {
            DataType::Decimal => properties::DataType::Decimal,
            DataType::String => properties::DataType::String,
            DataType::Boolean => properties::DataType::Boolean,
            DataType::Timestamp => properties::DataType::Timestamp,
            DataType::Int8 => properties::DataType::Int8,
            DataType::Int16 => properties::DataType::Int16,
            DataType::Int32 => properties::DataType::Int32,
            DataType::Int64 => properties::DataType::Int64,
            DataType::UInt8 => properties::DataType::UInt8,
            DataType::UInt16 => properties::DataType::UInt16,
            DataType::UInt32 => properties::DataType::UInt32,
            DataType::UInt64 => properties::DataType::UInt64,
            DataType::Float64 => properties::DataType::Float64,
        }
    }
}

impl TryInto<ArrowDataType> for DataType {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<ArrowDataType, Self::Error> {
        Ok(match self {
            DataType::Decimal => ArrowDataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE),
            DataType::String => ArrowDataType::Utf8,
            DataType::Boolean => ArrowDataType::Boolean,
            DataType::Timestamp => ArrowDataType::Timestamp(TIME_UNIT, None),
            DataType::Int8 => ArrowDataType::Int8,
            DataType::Int16 => ArrowDataType::Int16,
            DataType::Int32 => ArrowDataType::Int32,
            DataType::Int64 => ArrowDataType::Int64,
            DataType::UInt8 => ArrowDataType::UInt8,
            DataType::UInt16 => ArrowDataType::UInt16,
            DataType::UInt32 => ArrowDataType::UInt32,
            DataType::UInt64 => ArrowDataType::UInt64,
            DataType::Float64 => ArrowDataType::Float64,
        })
    }
}

impl TryInto<DataType> for ArrowDataType {
    type Error = PlatformError;

    fn try_into(self) -> Result<DataType, Self::Error> {
        Ok(match self {
            ArrowDataType::Boolean => DataType::Boolean,
            ArrowDataType::Timestamp(_, _) => DataType::Timestamp,
            ArrowDataType::Utf8 => DataType::String,
            ArrowDataType::Int8 => DataType::Int8,
            ArrowDataType::Int16 => DataType::Int16,
            ArrowDataType::Int32 => DataType::Int32,
            ArrowDataType::Int64 => DataType::Int64,
            ArrowDataType::UInt8 => DataType::UInt8,
            ArrowDataType::UInt16 => DataType::UInt16,
            ArrowDataType::UInt32 => DataType::UInt32,
            ArrowDataType::UInt64 => DataType::UInt64,
            ArrowDataType::Float64 => DataType::Float64,
            ArrowDataType::Decimal128(_, _) => DataType::Decimal,
            _ => return Err(PlatformError::EntityMap(format!("{:?}", self))),
        })
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum DictionaryType {
    #[serde(rename = "uint8")]
    UInt8,
    #[serde(rename = "uint16")]
    UInt16,
    #[serde(rename = "uint32")]
    UInt32,
    #[serde(rename = "uint64")]
    UInt64,
}

impl TryInto<properties::DictionaryType> for DictionaryType {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<properties::DictionaryType, Self::Error> {
        Ok(match self {
            DictionaryType::UInt8 => properties::DictionaryType::UInt8,
            DictionaryType::UInt16 => properties::DictionaryType::UInt16,
            DictionaryType::UInt32 => properties::DictionaryType::UInt32,
            DictionaryType::UInt64 => properties::DictionaryType::UInt64,
        })
    }
}

impl TryInto<DictionaryType> for properties::DictionaryType {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<DictionaryType, Self::Error> {
        Ok(match self {
            properties::DictionaryType::UInt8 => DictionaryType::UInt8,
            properties::DictionaryType::UInt16 => DictionaryType::UInt16,
            properties::DictionaryType::UInt32 => DictionaryType::UInt32,
            properties::DictionaryType::UInt64 => DictionaryType::UInt64,
            _ => return Err(PlatformError::EntityMap(format!("{:?}", self))),
        })
    }
}
