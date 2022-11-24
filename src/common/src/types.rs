use arrow::datatypes::DataType as ArrowDataType;
use datafusion::logical_plan::Operator;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;

use crate::error::CommonError;
use crate::ScalarValue;

pub const DECIMAL_PRECISION: usize = 19;
pub const DECIMAL_SCALE: usize = 10;

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum TimeUnit {
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}

impl TryInto<TimeUnit> for arrow::datatypes::TimeUnit {
    type Error = CommonError;

    fn try_into(self) -> std::result::Result<TimeUnit, Self::Error> {
        Ok(match self {
            arrow::datatypes::TimeUnit::Second => TimeUnit::Second,
            arrow::datatypes::TimeUnit::Millisecond => TimeUnit::Millisecond,
            arrow::datatypes::TimeUnit::Microsecond => TimeUnit::Microsecond,
            arrow::datatypes::TimeUnit::Nanosecond => TimeUnit::Nanosecond,
        })
    }
}

impl TryInto<arrow::datatypes::TimeUnit> for TimeUnit {
    type Error = CommonError;

    fn try_into(self) -> std::result::Result<arrow::datatypes::TimeUnit, Self::Error> {
        Ok(match self {
            TimeUnit::Second => arrow::datatypes::TimeUnit::Second,
            TimeUnit::Millisecond => arrow::datatypes::TimeUnit::Millisecond,
            TimeUnit::Microsecond => arrow::datatypes::TimeUnit::Microsecond,
            TimeUnit::Nanosecond => arrow::datatypes::TimeUnit::Nanosecond,
        })
    }
}

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
    type Error = CommonError;

    fn try_into(self) -> std::result::Result<ArrowDataType, Self::Error> {
        Ok(match self {
            DataType::Number => ArrowDataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE),
            DataType::String => ArrowDataType::Utf8,
            DataType::LargeUtf8 => ArrowDataType::LargeUtf8,
            DataType::Boolean => ArrowDataType::Boolean,
            DataType::Timestamp(tu) => ArrowDataType::Timestamp(tu.try_into()?, None),
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
    type Error = CommonError;

    fn try_into(self) -> std::result::Result<DataType, Self::Error> {
        Ok(match self {
            ArrowDataType::Boolean => DataType::Boolean,
            ArrowDataType::Utf8 => DataType::String,
            ArrowDataType::LargeUtf8 => DataType::LargeUtf8,
            ArrowDataType::Timestamp(tu, _) => DataType::Timestamp(tu.try_into()?),
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
            _ => return Err(CommonError::EntityMapping),
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
    type Error = CommonError;

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
    type Error = CommonError;

    fn try_into(self) -> std::result::Result<DictionaryDataType, Self::Error> {
        Ok(match self {
            ArrowDataType::UInt8 => DictionaryDataType::UInt8,
            ArrowDataType::UInt16 => DictionaryDataType::UInt16,
            ArrowDataType::UInt32 => DictionaryDataType::UInt32,
            ArrowDataType::UInt64 => DictionaryDataType::UInt64,
            _ => return Err(CommonError::EntityMapping),
        })
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Hash, Eq, PartialEq)]
pub enum PropertyRef {
    User(String),
    Event(String),
    Custom(u64),
}

impl PropertyRef {
    pub fn name(&self) -> String {
        match self {
            PropertyRef::User(name) => name.clone(),
            PropertyRef::Event(name) => name.clone(),
            PropertyRef::Custom(_id) => unimplemented!(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum EventRef {
    RegularName(String),
    // TODO remove this, use only pk(id) addressing
    Regular(u64),
    Custom(u64),
}

impl EventRef {
    pub fn name(&self) -> String {
        match self {
            EventRef::RegularName(name) => name.to_owned(),
            EventRef::Regular(id) => id.to_string(),
            EventRef::Custom(id) => id.to_string(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum PropValueOperation {
    Eq,
    Neq,
    Gt,
    Gte,
    Lt,
    Lte,
    True,
    False,
    Exists,
    Empty,
    ArrAll,
    ArrAny,
    ArrNone,
    Like,
    NotLike,
    Regex,
    NotRegex,
}

impl From<PropValueOperation> for Operator {
    fn from(pv: PropValueOperation) -> Self {
        match pv {
            PropValueOperation::Eq => Operator::Eq,
            PropValueOperation::Neq => Operator::NotEq,
            PropValueOperation::Like => Operator::Like,
            _ => panic!("unreachable"),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum EventFilter {
    Property {
        property: PropertyRef,
        operation: PropValueOperation,
        value: Option<Vec<ScalarValue>>,
    },
}

#[derive(Clone, PartialEq, PartialOrd, Eq, Ord, Debug, Hash)]
pub enum OptionalProperty<T> {
    None,
    Some(T),
}

impl<T> Default for OptionalProperty<T> {
    #[inline]
    fn default() -> OptionalProperty<T> {
        OptionalProperty::None
    }
}

impl<T> OptionalProperty<T> {
    pub fn insert(&mut self, v: T) {
        *self = OptionalProperty::Some(v)
    }

    pub fn is_none(&self) -> bool {
        matches!(self, OptionalProperty::None)
    }

    pub fn into<X>(self) -> OptionalProperty<X>
        where T: Into<X> {
        match self {
            OptionalProperty::None => OptionalProperty::None,
            OptionalProperty::Some(v) => OptionalProperty::Some(v.into()),
        }
    }

    pub fn try_into<X>(self) -> std::result::Result<OptionalProperty<X>, <T as TryInto<X>>::Error>
        where T: TryInto<X> {
        Ok(match self {
            OptionalProperty::None => OptionalProperty::None,
            OptionalProperty::Some(v) => OptionalProperty::Some(v.try_into()?),
        })
    }
}

impl<T> Serialize for OptionalProperty<T>
    where T: Serialize
{
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
        where S: Serializer {
        match self {
            OptionalProperty::None => panic!("!"),
            OptionalProperty::Some(v) => serializer.serialize_some(v),
        }
    }
}

impl<'de, T> Deserialize<'de> for OptionalProperty<T>
    where T: Deserialize<'de>
{
    fn deserialize<D>(de: D) -> std::result::Result<Self, D::Error>
        where D: Deserializer<'de> {
        let a = Deserialize::deserialize(de);
        a.map(OptionalProperty::Some)
    }
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;
    use serde_json;

    use crate::types::OptionalProperty;

    #[test]
    fn test_optional_property_with_option() -> Result<(), serde_json::Error> {
        #[derive(Deserialize)]
        struct Test {
            #[serde(default)]
            v: OptionalProperty<Option<bool>>,
        }

        assert_eq!(
            serde_json::from_str::<Test>(r#"{}"#)?.v,
            OptionalProperty::None
        );
        assert_eq!(
            serde_json::from_str::<Test>(r#"{"v":null}"#)?.v,
            OptionalProperty::Some(None)
        );
        assert_eq!(
            serde_json::from_str::<Test>(r#"{"v":true}"#)?.v,
            OptionalProperty::Some(Some(true))
        );

        Ok(())
    }

    #[test]
    fn test_optional_property() -> Result<(), serde_json::Error> {
        #[derive(Deserialize)]
        struct Test {
            #[serde(default)]
            v: OptionalProperty<bool>,
        }

        assert_eq!(
            serde_json::from_str::<Test>(r#"{}"#)?.v,
            OptionalProperty::None
        );
        assert!(serde_json::from_str::<Test>(r#"{"v":null}"#).is_err());
        assert_eq!(
            serde_json::from_str::<Test>(r#"{"v":true}"#)?.v,
            OptionalProperty::Some(true)
        );

        Ok(())
    }
}
