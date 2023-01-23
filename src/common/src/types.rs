use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;

pub const DECIMAL_PRECISION: u8 = 19;
pub const DECIMAL_SCALE: i8 = 10;

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
