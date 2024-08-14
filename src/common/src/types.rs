use std::string::ToString;

use arrow::datatypes;
use arrow2::datatypes::DataType as DataType2;
use arrow_schema::DataType;
use arrow_schema::TimeUnit;
use lazy_static::lazy_static;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;

use crate::error::CommonError;

pub const DECIMAL_PRECISION: u8 = 28;
pub const DECIMAL_SCALE: i8 = 16;
pub const DECIMAL_MULTIPLIER: i128 = 10i128.pow(DECIMAL_SCALE as u32);
pub const ROUND_DIGITS: u8 = 3;
pub const TIME_UNIT: TimeUnit = TimeUnit::Millisecond;

pub const TABLE_EVENTS: &str = "events";

pub const COLUMN_PROJECT_ID: &str = "project_id";
pub const COLUMN_CREATED_AT: &str = "created_at";
pub const COLUMN_EVENT_ID: &str = "event_id";
pub const COLUMN_EVENT: &str = "event";
pub const COLUMN_IP: &str = "ip";
pub const COLUMN_SEGMENT: &str = "segment";
pub const GROUP_COLUMN_PROJECT_ID: &str = "project_id";
pub const GROUP_COLUMN_ID: &str = "id";
pub const GROUP_COLUMN_VERSION: &str = "version";
pub const GROUP_COLUMN_CREATED_AT: &str = "created_at";

pub const EVENT_PROPERTY_NAME: &str = "Name";
pub const EVENT_PROPERTY_HREF: &str = "Href";
pub const EVENT_PROPERTY_ID: &str = "ID";
pub const EVENT_PROPERTY_CLASS: &str = "Class";
pub const EVENT_PROPERTY_TEXT: &str = "Text";
pub const EVENT_PROPERTY_ELEMENT: &str = "Element";
pub const EVENT_PROPERTY_PAGE_PATH: &str = "Page Path";
pub const EVENT_PROPERTY_PAGE_REFERER: &str = "Page Referer";
pub const EVENT_PROPERTY_PAGE_SEARCH: &str = "Page Search";
pub const EVENT_PROPERTY_PAGE_TITLE: &str = "Page Title";
pub const EVENT_PROPERTY_PAGE_URL: &str = "Page URL";
pub const EVENT_PROPERTY_SESSION_LENGTH: &str = "Session Length";

pub const EVENT_PROPERTY_CLIENT_FAMILY: &str = "Client Family";
pub const EVENT_PROPERTY_CLIENT_VERSION_MINOR: &str = "Client Version Minor";
pub const EVENT_PROPERTY_CLIENT_VERSION_MAJOR: &str = "Client Version Major";
pub const EVENT_PROPERTY_CLIENT_VERSION_PATCH: &str = "Version Patch";
pub const EVENT_PROPERTY_DEVICE_FAMILY: &str = "Device Family";
pub const EVENT_PROPERTY_DEVICE_BRAND: &str = "Device Brand";
pub const EVENT_PROPERTY_DEVICE_MODEL: &str = "Device Model";
pub const EVENT_PROPERTY_OS: &str = "OS";
pub const EVENT_PROPERTY_OS_FAMILY: &str = "OS Family";
pub const EVENT_PROPERTY_OS_VERSION_MAJOR: &str = "OS Version Major";
pub const EVENT_PROPERTY_OS_VERSION_MINOR: &str = "OS Version Minor";
pub const EVENT_PROPERTY_OS_VERSION_PATCH: &str = "OS Version Patch";
pub const EVENT_PROPERTY_OS_VERSION_PATCH_MINOR: &str = "OS Version Patch Minor";
pub const EVENT_PROPERTY_COUNTRY: &str = "Country";
pub const EVENT_PROPERTY_CITY: &str = "City";
pub const EVENT_PROPERTY_UTM_SOURCE: &str = "UTM Source";
pub const EVENT_PROPERTY_UTM_MEDIUM: &str = "UTM Medium";
pub const EVENT_PROPERTY_UTM_CAMPAIGN: &str = "UTM Campaign";
pub const EVENT_PROPERTY_UTM_TERM: &str = "UTM Term";
pub const EVENT_PROPERTY_UTM_CONTENT: &str = "UTM Content";

pub const EVENT_CLICK: &str = "Click";
pub const EVENT_PAGE: &str = "Page";
pub const EVENT_SCREEN: &str = "Screen";
pub const EVENT_SESSION_BEGIN: &str = "Session Begin";
pub const EVENT_SESSION_END: &str = "Session End";

pub const RESERVED_COLUMN_FUNNEL_TOTAL: &str = "total";
pub const RESERVED_COLUMN_FUNNEL_COMPLETED: &str = "completed";
pub const RESERVED_COLUMN_FUNNEL_CONVERSION_RATIO: &str = "conversion_ratio";
pub const RESERVED_COLUMN_FUNNEL_AVG_TIME_TO_CONVERT: &str = "avg_time_to_convert";
pub const RESERVED_COLUMN_FUNNEL_AVG_TIME_TO_CONVERT_FROM_START: &str = "avg_time_to_convert_from_start";
pub const RESERVED_COLUMN_FUNNEL_DROPPED_OFF: &str = "dropped_off";
pub const RESERVED_COLUMN_FUNNEL_DROP_OFF_RATIO: &str = "drop_off_ratio";
pub const RESERVED_COLUMN_AGG_PARTITIONED_AGGREGATE: &str = "partitioned_agg";
pub const RESERVED_COLUMN_AGG_PARTITIONED_COUNT: &str = "partitioned_count";
pub const RESERVED_COLUMN_AGG: &str = "agg";
pub const RESERVED_COLUMN_COUNT: &str = "count";

pub const METRIC_STORE_INSERTS_TOTAL: &str = "optiprism_store_inserts_total";
pub const METRIC_STORE_INSERT_TIME_SECONDS: &str = "optiprism_store_insert_time_seconds";
pub const METRIC_STORE_SCANS_TOTAL: &str = "optiprism_store_scans_total";
pub const METRIC_STORE_SCAN_TIME_SECONDS: &str = "optiprism_store_scan_time_seconds";
pub const METRIC_STORE_SCAN_PARTS: &str = "optiprism_store_scan_parts";
pub const METRIC_STORE_PARTS: &str = "optiprism_store_parts";
pub const METRIC_STORE_PARTS_SIZE_BYTES: &str = "optiprism_store_parts_size_bytes";
pub const METRIC_STORE_PART_SIZE_BYTES: &str = "optiprism_store_part_size_bytes";
pub const METRIC_STORE_PART_VALUES: &str = "optiprism_store_part_values";
pub const METRIC_STORE_PARTS_VALUES: &str = "optiprism_store_parts_values";
pub const METRIC_STORE_MERGES_TOTAL: &str = "optiprism_store_merges_total";
pub const METRIC_STORE_MERGE_TIME_SECONDS: &str = "optiprism_store_merge_time_seconds";
pub const METRIC_STORE_TABLE_FIELDS: &str = "optiprism_store_table_fields";
pub const METRIC_STORE_MEMTABLE_ROWS: &str = "optiprism_store_memtable_rows";
pub const METRIC_STORE_SCAN_MEMTABLE_SECONDS: &str = "optiprism_store_scan_memtable_seconds";
pub const METRIC_STORE_COMPACTIONS_TOTAL: &str = "optiprism_store_compactions_total";
pub const METRIC_STORE_LEVEL_COMPACTION_TIME_SECONDS: &str = "optiprism_store_level_compaction_time_seconds";
pub const METRIC_STORE_COMPACTION_TIME_SECONDS: &str = "optiprism_store_compaction_time_seconds";
pub const METRIC_STORE_RECOVERY_TIME_SECONDS: &str = "optiprism_store_recovery_time_seconds";
pub const METRIC_STORE_FLUSH_TIME_SECONDS: &str = "optiprism_store_flush_time_seconds";
pub const METRIC_STORE_FLUSHES_TOTAL: &str = "optiprism_store_flushes_total";

pub const METRIC_INGESTER_TRACKED_TOTAL: &str = "optiprism_ingester_tracked_total";
pub const METRIC_INGESTER_TRACK_TIME_SECONDS: &str = "optiprism_ingester_track_time_seconds";
pub const METRIC_INGESTER_IDENTIFIED_TOTAL: &str = "optiprism_ingester_identified_total";
pub const METRIC_INGESTER_IDENTIFY_TIME_SECONDS: &str = "optiprism_ingester_identify_time_seconds";

pub const METRIC_QUERY_QUERIES_TOTAL: &str = "optiprism_query_queries_total";
pub const METRIC_QUERY_EXECUTION_TIME_SECONDS: &str = "optiprism_query_execution_time_seconds";

pub const METRIC_HTTP_REQUEST_TIME_SECONDS: &str = "optiprism_http_request_time_seconds";
pub const METRIC_HTTP_REQUESTS_TOTAL: &str = "optiprism_http_requests_total";

pub const METRIC_BACKUPS_TOTAL: &str = "optiprism_query_backups_total";
pub const METRIC_BACKUP_TIME_SECONDS: &str = "optiprism_backup_time_seconds";

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
#[derive(Default)]
pub enum DType {
    #[default]
    String,
    Int8,
    Int16,
    Int32,
    Int64,
    Decimal,
    Boolean,
    Timestamp,
    List(Box<DType>),
}

impl DType {
    pub fn short_name(&self) -> String {
        match self {
            DType::String => "str".to_string(),
            DType::Int8 => "i8".to_string(),
            DType::Int16 => "i16".to_string(),
            DType::Int32 => "i32".to_string(),
            DType::Int64 => "i64".to_string(),
            DType::Decimal => "d".to_string(),
            DType::Boolean => "b".to_string(),
            DType::Timestamp => "ts".to_string(),
            DType::List(v) => {
                let s = match v.as_ref() {
                    DType::String => "str".to_string(),
                    DType::Int8 => "i8".to_string(),
                    DType::Int16 => "i16".to_string(),
                    DType::Int32 => "i32".to_string(),
                    DType::Int64 => "i64".to_string(),
                    DType::Decimal => "d".to_string(),
                    DType::Boolean => "b".to_string(),
                    DType::Timestamp => "ts".to_string(),
                    _ => unimplemented!(),
                };
                format!("l_{}", s)
            }
        }
    }
}

impl From<DType> for datatypes::DataType {
    fn from(value: DType) -> Self {
        match value {
            DType::String => datatypes::DataType::Utf8,
            DType::Int8 => datatypes::DataType::Int8,
            DType::Int16 => datatypes::DataType::Int16,
            DType::Int32 => datatypes::DataType::Int32,
            DType::Int64 => datatypes::DataType::Int64,
            DType::Decimal => datatypes::DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE),
            DType::Boolean => datatypes::DataType::Boolean,
            DType::Timestamp => datatypes::DataType::Timestamp(TIME_UNIT, None),
            DType::List(dt) => match dt.as_ref() {
                DType::String => DataType::Utf8,
                DType::Int8 => DataType::Int8,
                DType::Int16 => DataType::Int16,
                DType::Int32 => DataType::Int32,
                DType::Int64 => DataType::Int64,
                DType::Decimal => DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE),
                DType::Boolean => DataType::Boolean,
                DType::Timestamp => DataType::Timestamp(TIME_UNIT, None),
                _ => unreachable!("Unsupported type"),
            },
        }
    }
}

impl From<DataType> for DType {
    fn from(dt: DataType) -> Self {
        match dt {
            DataType::Boolean => DType::Boolean,
            DataType::Int8 => DType::Int8,
            DataType::Int16 => DType::Int16,
            DataType::Int32 => DType::Int32,
            DataType::Int64 => DType::Int64,
            DataType::Utf8 => DType::String,
            DataType::Decimal128(_, _) => DType::Decimal,
            DataType::Timestamp(_, _) => DType::Timestamp,
            DataType::List(f) => match f.data_type() {
                DataType::Boolean => DType::Boolean,
                DataType::Int8 => DType::Int8,
                DataType::Int16 => DType::Int16,
                DataType::Int32 => DType::Int32,
                DataType::Int64 => DType::Int64,
                DataType::Utf8 => DType::String,
                DataType::Decimal128(_, _) => DType::Decimal,
                DataType::Timestamp(_, _) => DType::Timestamp,
                _ => unreachable!("unsupported list type"),
            },
            _ => unreachable!("unsupported type"),
        }
    }
}

impl TryFrom<DType> for DataType2 {
    type Error = CommonError;
    fn try_from(value: DType) -> Result<Self, Self::Error> {
        Ok(match value {
            DType::String => DataType2::Utf8,
            DType::Int8 => DataType2::Int8,
            DType::Int16 => DataType2::Int16,
            DType::Int32 => DataType2::Int32,
            DType::Int64 => DataType2::Int64,
            DType::Decimal => {
                DataType2::Decimal(DECIMAL_PRECISION as usize, DECIMAL_SCALE as usize)
            }
            DType::Boolean => DataType2::Boolean,
            DType::Timestamp => {
                DataType2::Timestamp(arrow2::datatypes::TimeUnit::Millisecond, None)
            }
            DType::List(dt) => match dt.as_ref() {
                DType::String => DataType2::Utf8,
                DType::Int8 => DataType2::Int8,
                DType::Int16 => DataType2::Int16,
                DType::Int32 => DataType2::Int32,
                DType::Int64 => DataType2::Int64,
                DType::Decimal => {
                    DataType2::Decimal(DECIMAL_PRECISION as usize, DECIMAL_SCALE as usize)
                }
                DType::Boolean => DataType2::Boolean,
                DType::Timestamp => {
                    DataType2::Timestamp(arrow2::datatypes::TimeUnit::Millisecond, None)
                }
                _ => return Err(CommonError::General("Unsupported type4".to_string())),
            },
        })
    }
}

impl TryFrom<DataType2> for DType {
    type Error = CommonError;

    fn try_from(dt: DataType2) -> Result<Self, Self::Error> {
        Ok(match dt {
            DataType2::Boolean => DType::Boolean,
            DataType2::Int8 => DType::Int8,
            DataType2::Int16 => DType::Int16,
            DataType2::Int32 => DType::Int32,
            DataType2::Int64 => DType::Int64,
            DataType2::Timestamp(_, _) => DType::Timestamp,
            DataType2::Utf8 => DType::String,
            DataType2::Decimal(_, _) => DType::Decimal,
            DataType2::List(f) => match f.data_type() {
                DataType2::Boolean => DType::Boolean,
                DataType2::Int8 => DType::Int8,
                DataType2::Int16 => DType::Int16,
                DataType2::Int32 => DType::Int32,
                DataType2::Int64 => DType::Int64,
                DataType2::Timestamp(_, _) => DType::Timestamp,
                DataType2::Utf8 => DType::String,
                DataType2::Decimal(_, _) => DType::Decimal,
                _ => return Err(CommonError::General("Unsupported type5".to_string())),
            },
            _ => {
                return Err(CommonError::General(format!("Unsupported type {:?}", dt)));
            }
        })
    }
}

#[derive(Clone, PartialEq, PartialOrd, Eq, Ord, Debug, Hash, Default)]
pub enum OptionalProperty<T> {
    #[default]
    None,
    Some(T),
}

impl<T> OptionalProperty<T> {
    pub fn insert(&mut self, v: T) {
        *self = OptionalProperty::Some(v)
    }

    pub fn is_none(&self) -> bool {
        matches!(self, OptionalProperty::None)
    }

    pub fn into<X>(self) -> OptionalProperty<X>
    where
        T: Into<X>,
    {
        match self {
            OptionalProperty::None => OptionalProperty::None,
            OptionalProperty::Some(v) => OptionalProperty::Some(v.into()),
        }
    }

    pub fn try_into<X>(self) -> std::result::Result<OptionalProperty<X>, <T as TryInto<X>>::Error>
    where
        T: TryInto<X>,
    {
        Ok(match self {
            OptionalProperty::None => OptionalProperty::None,
            OptionalProperty::Some(v) => OptionalProperty::Some(v.try_into()?),
        })
    }

    pub fn map<F, U>(self, f: F) -> OptionalProperty<U>
    where
        F: FnOnce(T) -> U,
    {
        match self {
            OptionalProperty::None => OptionalProperty::None,
            OptionalProperty::Some(v) => OptionalProperty::Some(f(v)),
        }
    }
}

impl<T> Serialize for OptionalProperty<T>
where
    T: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            OptionalProperty::None => panic!("!"),
            OptionalProperty::Some(v) => serializer.serialize_some(v),
        }
    }
}

impl<'de, T> Deserialize<'de> for OptionalProperty<T>
where
    T: Deserialize<'de>,
{
    fn deserialize<D>(de: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let a = Deserialize::deserialize(de);
        a.map(OptionalProperty::Some)
    }
}
#[derive(Serialize, Deserialize, Clone, PartialEq, PartialOrd, Eq, Ord, Debug, Hash, Default)]
#[serde(rename_all = "camelCase")]
pub enum SortDirection {
    #[default]
    Asc,
    Desc,
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
