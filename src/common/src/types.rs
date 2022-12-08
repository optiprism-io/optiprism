use datafusion::logical_expr::Operator;
use datafusion_common::ScalarValue;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;
use crate::scalar::ScalarValueRef;
use crate::error::CommonError;
use serde_with::{serde_as};

pub const DECIMAL_PRECISION: u8 = 19;
pub const DECIMAL_SCALE: i8 = 10;

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum TimeUnit {
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}

impl TryInto<TimeUnit> for arrow_schema::TimeUnit {
    type Error = CommonError;

    fn try_into(self) -> std::result::Result<TimeUnit, Self::Error> {
        Ok(match self {
            arrow_schema::TimeUnit::Second => TimeUnit::Second,
            arrow_schema::TimeUnit::Millisecond => TimeUnit::Millisecond,
            arrow_schema::TimeUnit::Microsecond => TimeUnit::Microsecond,
            arrow_schema::TimeUnit::Nanosecond => TimeUnit::Nanosecond,
        })
    }
}

impl TryInto<arrow_schema::TimeUnit> for TimeUnit {
    type Error = CommonError;

    fn try_into(self) -> std::result::Result<arrow_schema::TimeUnit, Self::Error> {
        Ok(match self {
            TimeUnit::Second => arrow_schema::TimeUnit::Second,
            TimeUnit::Millisecond => arrow_schema::TimeUnit::Millisecond,
            TimeUnit::Microsecond => arrow_schema::TimeUnit::Microsecond,
            TimeUnit::Nanosecond => arrow_schema::TimeUnit::Nanosecond,
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

#[serde_as]
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum EventFilter {
    Property {
        property: PropertyRef,
        operation: PropValueOperation,
        #[serde_as(as = "Option<Vec<ScalarValueRef>>")]
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
