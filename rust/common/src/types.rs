use crate::ScalarValue;
use arrow::datatypes::DataType as ArrowDataType;
use arrow::datatypes::TimeUnit::Second;
use datafusion::logical_plan::Operator;
use serde::{Deserialize, Serialize};

pub const DECIMAL_PRECISION: usize = 19;
pub const DECIMAL_SCALE: usize = 10;

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub enum DataType {
    Number,
    String,
    Boolean,
    Timestamp,
}

impl DataType {
    pub fn to_arrow(self) -> ArrowDataType {
        match self {
            DataType::Number => ArrowDataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE),
            DataType::String => ArrowDataType::Utf8,
            DataType::Boolean => ArrowDataType::Boolean,
            DataType::Timestamp => ArrowDataType::Timestamp(Second, None),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub enum DictionaryDataType {
    UInt8,
    UInt16,
    UInt32,
    UInt64,
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

pub type OptionalProperty<T> = Option<T>;
