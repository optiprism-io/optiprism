#![feature(let_chains)]
extern crate core;

pub mod accounts;
pub mod auth;
pub mod context;
pub mod custom_events;
pub mod dashboards;
// pub mod datatype;
pub mod error;
pub mod event_records;
pub mod events;
mod group_records;
mod groups;
pub mod http;
pub mod organizations;
pub mod projects;
pub mod properties;
pub mod reports;
pub mod event_segmentation;
mod funnel;
mod bookmarks;
// pub mod stub;

use std::fmt::Debug;
use std::sync::Arc;

use arrow::array::Array;
use arrow::array::ArrayRef;
use arrow::array::BooleanArray;
use arrow::array::Decimal128Array;
use arrow::array::Float32Array;
use arrow::array::Float64Array;
use arrow::array::Int16Array;
use arrow::array::Int32Array;
use arrow::array::Int64Array;
use arrow::array::Int8Array;
use arrow::array::StringArray;
use arrow::array::TimestampMillisecondArray;
use arrow::array::UInt16Array;
use arrow::array::UInt32Array;
use arrow::array::UInt64Array;
use arrow::array::UInt8Array;
use arrow::datatypes::TimeUnit;
use chrono::{DateTime, Utc};
use common::startup_config::StartupConfig;
use common::types::DType;
use common::types::SortDirection;
use common::types::ROUND_DIGITS;
use common::types::TIME_UNIT;
use common::DECIMAL_PRECISION;
use common::DECIMAL_SCALE;
use common::GROUPS_COUNT;
pub use context::Context;
use convert_case::Case;
use convert_case::Casing;
use datafusion_common::ScalarValue;
pub use error::PlatformError;
pub use error::Result;
use metadata::MetadataProvider;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde::Deserialize;
use serde::Serialize;
use serde_json::json;
use serde_json::Number;
use serde_json::Value;
use query::event_records::EventRecordsProvider;
use query::group_records::GroupRecordsProvider;
use query::properties::PropertiesProvider;

use crate::accounts::Accounts;
use crate::auth::Auth;
use crate::bookmarks::Bookmarks;
use crate::custom_events::CustomEvents;
use crate::dashboards::Dashboards;
use crate::event_records::EventRecords;
use crate::event_segmentation::{EventSegmentation};
use crate::events::Events;
use crate::funnel::Funnel;
use crate::group_records::GroupRecords;
use crate::groups::Groups;
use crate::organizations::Organizations;
use crate::projects::Projects;
use crate::properties::Properties;
use crate::reports::Reports;

pub struct PlatformProvider {
    pub events: Arc<Events>,
    pub custom_events: Arc<CustomEvents>,
    pub groups: Arc<Groups>,
    pub event_properties: Arc<Properties>,
    pub group_properties: Vec<Arc<Properties>>,
    pub accounts: Arc<Accounts>,
    pub auth: Arc<Auth>,
    pub event_segmentation: Arc<EventSegmentation>,
    pub funnel: Arc<Funnel>,
    pub dashboards: Arc<Dashboards>,
    pub reports: Arc<Reports>,
    pub bookmarks: Arc<Bookmarks>,
    pub projects: Arc<Projects>,
    pub organizations: Arc<Organizations>,
    pub event_records: Arc<EventRecords>,
    pub group_records: Arc<GroupRecords>,
}

impl PlatformProvider {
    pub fn new(
        md: Arc<MetadataProvider>,
        es_prov: Arc<query::event_segmentation::EventSegmentationProvider>,
        funnel_prov: Arc<query::funnel::FunnelProvider>,
        prop_prov: Arc<PropertiesProvider>,
        event_records_prov: Arc<EventRecordsProvider>,
        group_records_prov: Arc<GroupRecordsProvider>,
        cfg: StartupConfig,
    ) -> Self {
        let group_properties = (0..GROUPS_COUNT)
            .map(|gid| Arc::new(Properties::new_group(md.group_properties[gid].clone(), prop_prov.clone())))
            .collect::<Vec<_>>();
        Self {
            events: Arc::new(Events::new(md.events.clone())),
            custom_events: Arc::new(CustomEvents::new(md.custom_events.clone())),
            groups: Arc::new(Groups::new(md.groups.clone())),
            event_properties: Arc::new(Properties::new_event(md.event_properties.clone(), prop_prov.clone())),
            group_properties,
            accounts: Arc::new(Accounts::new(md.accounts.clone())),
            auth: Arc::new(Auth::new(
                md.clone(),
                cfg.clone(),
            )),
            event_segmentation: Arc::new(EventSegmentation::new(md.clone(), es_prov)),
            funnel: Arc::new(Funnel::new(md.clone(), funnel_prov)),
            dashboards: Arc::new(Dashboards::new(md.dashboards.clone())),
            reports: Arc::new(Reports::new(md.reports.clone())),
            event_records: Arc::new(EventRecords::new(md.clone(), event_records_prov)),
            group_records: Arc::new(GroupRecords::new(md.clone(), group_records_prov)),
            projects: Arc::new(Projects::new(md.clone(), cfg.clone())),
            organizations: Arc::new(Organizations::new(md.clone(), cfg.clone())),
            bookmarks: Arc::new(Bookmarks::new(md.bookmarks.clone())),
        }
    }
}

#[macro_export]
macro_rules! arr_to_json_values {
    ($array_ref:expr,$array_type:ident) => {{
        let arr = $array_ref.as_any().downcast_ref::<$array_type>().unwrap();
        arr.iter().map(|value| json!(value)).collect()
    }};
}

#[macro_export]
macro_rules! int_arr_to_json_values {
    ($array_ref:expr,$array_type:ident) => {{
        let arr = $array_ref.as_any().downcast_ref::<$array_type>().unwrap();
        arr.iter()
            .map(|value| match value {
                None => Value::Number(Number::from_f64(0.0).unwrap()),
                Some(v) => json!(v),
            })
            .collect()
    }};
}


pub fn scalar_to_json(v: &ScalarValue) -> Value {
    if v.is_null() {
        return Value::Null;
    }
    match v {
        ScalarValue::Boolean(Some(v)) => Value::Bool(*v),
        ScalarValue::Decimal128(Some(v), _, _) => {
            let d = Decimal::from_i128_with_scale(*v, DECIMAL_SCALE as u32)
                .round_dp(ROUND_DIGITS.into());
            let d_f = match d.to_f64() {
                None => {
                    panic!("can't convert decimal to f64");
                }
                Some(v) => v,
            };
            let n = match Number::from_f64(d_f) {
                None => {
                    panic!("can't make json number from f64");
                }
                Some(v) => v,
            };
            Value::Number(n)
        }
        ScalarValue::Int8(Some(v)) => Value::Number(Number::from(*v)),
        ScalarValue::Int16(Some(v)) => Value::Number(Number::from(*v)),
        ScalarValue::Int32(Some(v)) => Value::Number(Number::from(*v)),
        ScalarValue::Int64(Some(v)) => Value::Number(Number::from(*v)),
        ScalarValue::Utf8(Some(v)) => Value::String(v.to_owned()),
        ScalarValue::TimestampMillisecond(Some(v), _) => Value::Number(Number::from(*v)),
        _ => unimplemented!()
    }
}
pub fn array_ref_to_json_values(arr: &ArrayRef) -> Vec<Value> {
    match arr.data_type() {
        arrow::datatypes::DataType::Int8 => int_arr_to_json_values!(arr, Int8Array),
        arrow::datatypes::DataType::Int16 => int_arr_to_json_values!(arr, Int16Array),
        arrow::datatypes::DataType::Int32 => int_arr_to_json_values!(arr, Int32Array),
        arrow::datatypes::DataType::Int64 => int_arr_to_json_values!(arr, Int64Array),
        arrow::datatypes::DataType::UInt8 => int_arr_to_json_values!(arr, UInt8Array),
        arrow::datatypes::DataType::UInt16 => int_arr_to_json_values!(arr, UInt16Array),
        arrow::datatypes::DataType::UInt32 => int_arr_to_json_values!(arr, UInt32Array),
        arrow::datatypes::DataType::UInt64 => int_arr_to_json_values!(arr, UInt64Array),
        arrow::datatypes::DataType::Float32 => {
            let arr = arr.as_any().downcast_ref::<Float32Array>().unwrap();
            arr.iter()
                .map(|value| {
                    match value {
                        None => Value::Number(Number::from_f64(0.0).unwrap()),
                        Some(_) => {
                            // https://stackoverflow.com/questions/73871891/how-to-serialize-a-struct-containing-f32-using-serde-json
                            json!(value.map(|v| (v as f64 * 1000000.0).trunc() / 1000000.0))
                        }
                    }
                })
                .collect()

            // arr_to_json_values!(arr, Float32Array)
        }
        arrow::datatypes::DataType::Float64 => int_arr_to_json_values!(arr, Float64Array),
        arrow::datatypes::DataType::Boolean => arr_to_json_values!(arr, BooleanArray),
        arrow::datatypes::DataType::Utf8 => arr_to_json_values!(arr, StringArray),
        arrow::datatypes::DataType::Timestamp(TIME_UNIT, _) => {
            arr_to_json_values!(arr, TimestampMillisecondArray)
        }
        arrow::datatypes::DataType::Decimal128(_, _s) => {
            let arr = arr.as_any().downcast_ref::<Decimal128Array>().unwrap();
            arr.iter()
                .map(|value| match value {
                    None => Value::Number(Number::from_f64(0.0).unwrap()),
                    Some(v) => {
                        let d = Decimal::from_i128_with_scale(v, DECIMAL_SCALE as u32)
                            .round_dp(ROUND_DIGITS.into());
                        let d_f = match d.to_f64() {
                            None => {
                                panic!("can't convert decimal to f64");
                            }
                            Some(v) => v,
                        };
                        let n = match Number::from_f64(d_f) {
                            None => {
                                panic!("can't make json number from f64");
                            }
                            Some(v) => v,
                        };
                        Value::Number(n)
                    }
                })
                .collect::<Vec<_>>()
        }
        _ => unimplemented!("{}", arr.data_type()),
    }
}

#[derive(Serialize, Deserialize, Default, Debug, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ResponseMetadata {
    pub next: Option<String>,
}

impl From<metadata::metadata::ResponseMetadata> for ResponseMetadata {
    fn from(value: metadata::metadata::ResponseMetadata) -> Self {
        ResponseMetadata { next: value.next }
    }
}

#[derive(Serialize, Deserialize, Default, Debug, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ListResponse<T>
where
    T: Debug,
{
    pub data: Vec<T>,
    pub meta: ResponseMetadata,
}

impl<A, B> Into<ListResponse<A>> for metadata::metadata::ListResponse<B>
where
    A: Debug,
    B: Into<A> + Clone + Debug,
{
    fn into(self) -> ListResponse<A> {
        let data = self.data.into_iter().map(|v| v.into()).collect::<Vec<A>>();
        let meta = ResponseMetadata {
            next: self.meta.next,
        };
        ListResponse { data, meta }
    }
}

pub fn json_value_to_scalar(v: &Value) -> ScalarValue {
    match v {
        Value::Bool(v) => ScalarValue::Boolean(Some(*v)),
        Value::Number(n) => {
            let dec = Decimal::try_from(n.as_f64().unwrap()).unwrap();
            ScalarValue::Decimal128(Some(dec.mantissa()), DECIMAL_PRECISION, dec.scale() as i8)
        }
        Value::String(v) => ScalarValue::Utf8(Some(v.to_string())),
        _ => unreachable!("unexpected value"),
    }
}

pub fn scalar_to_json_value(v: &ScalarValue) -> Value {
    match v {
        ScalarValue::Decimal128(None, _, _) => Value::Null,
        ScalarValue::Boolean(None) => Value::Null,
        ScalarValue::Utf8(None) => Value::Null,
        ScalarValue::Decimal128(Some(v), _p, s) => Value::Number(
            Number::from_f64(Decimal::new(*v as i64, *s as u32).to_f64().unwrap()).unwrap(),
        ),
        ScalarValue::Boolean(Some(v)) => Value::Bool(*v),
        ScalarValue::Utf8(Some(v)) => Value::String(v.to_owned()),
        _ => unreachable!("unexpected value"),
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
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
    Regex,
    Like,
    NotLike,
    NotRegex,
}

impl Into<common::query::PropValueOperation> for PropValueOperation {
    fn into(self) -> common::query::PropValueOperation {
        match self {
            PropValueOperation::Eq => common::query::PropValueOperation::Eq,
            PropValueOperation::Neq => common::query::PropValueOperation::Neq,
            PropValueOperation::Gt => common::query::PropValueOperation::Gt,
            PropValueOperation::Gte => common::query::PropValueOperation::Gte,
            PropValueOperation::Lt => common::query::PropValueOperation::Lt,
            PropValueOperation::Lte => common::query::PropValueOperation::Lte,
            PropValueOperation::True => common::query::PropValueOperation::True,
            PropValueOperation::False => common::query::PropValueOperation::False,
            PropValueOperation::Exists => common::query::PropValueOperation::Exists,
            PropValueOperation::Empty => common::query::PropValueOperation::Empty,
            PropValueOperation::Regex => common::query::PropValueOperation::Regex,
            PropValueOperation::Like => common::query::PropValueOperation::Like,
            PropValueOperation::NotLike => common::query::PropValueOperation::NotLike,
            PropValueOperation::NotRegex => common::query::PropValueOperation::NotRegex,
        }
    }
}

impl Into<PropValueOperation> for common::query::PropValueOperation {
    fn into(self) -> PropValueOperation {
        match self {
            common::query::PropValueOperation::Eq => PropValueOperation::Eq,
            common::query::PropValueOperation::Neq => PropValueOperation::Neq,
            common::query::PropValueOperation::Gt => PropValueOperation::Gt,
            common::query::PropValueOperation::Gte => PropValueOperation::Gte,
            common::query::PropValueOperation::Lt => PropValueOperation::Lt,
            common::query::PropValueOperation::Lte => PropValueOperation::Lte,
            common::query::PropValueOperation::True => PropValueOperation::True,
            common::query::PropValueOperation::False => PropValueOperation::False,
            common::query::PropValueOperation::Exists => PropValueOperation::Exists,
            common::query::PropValueOperation::Empty => PropValueOperation::Empty,
            common::query::PropValueOperation::Regex => PropValueOperation::Regex,
            common::query::PropValueOperation::Like => PropValueOperation::Like,
            common::query::PropValueOperation::NotLike => PropValueOperation::NotLike,
            common::query::PropValueOperation::NotRegex => PropValueOperation::NotRegex,
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(tag = "eventType", rename_all = "camelCase")]
pub enum EventRef {
    #[serde(rename_all = "camelCase")]
    Regular { event_name: String },
    #[serde(rename_all = "camelCase")]
    Custom { event_id: u64 },
}

impl EventRef {
    pub fn name(&self, idx: usize) -> String {
        match self {
            EventRef::Regular { event_name } => {
                format!("{}_regular_{}", event_name.to_case(Case::Snake), idx)
            }
            EventRef::Custom { event_id } => format!("{event_id}_custom_{idx}"),
        }
    }
}

impl From<EventRef> for common::query::EventRef {
    fn from(e: EventRef) -> Self {
        match e {
            EventRef::Regular { event_name } => common::query::EventRef::RegularName(event_name),
            EventRef::Custom { event_id } => common::query::EventRef::Custom(event_id),
        }
    }
}

impl Into<EventRef> for common::query::EventRef {
    fn into(self) -> EventRef {
        match self {
            common::query::EventRef::RegularName(name) => EventRef::Regular { event_name: name },
            common::query::EventRef::Regular(_id) => unimplemented!(),
            common::query::EventRef::Custom(id) => EventRef::Custom { event_id: id },
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq, Hash)]
#[serde(tag = "propertyType", rename_all = "camelCase")]
pub enum PropertyRef {
    #[serde(rename_all = "camelCase")]
    Group { property_name: String, group: usize },
    #[serde(rename_all = "camelCase")]
    Event { property_name: String },
    #[serde(rename_all = "camelCase")]
    Custom { property_id: u64 },
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq, Hash)]
#[serde(tag = "propertyType", rename_all = "camelCase")]
pub enum SortablePropertyRef {
    #[serde(rename_all = "camelCase")]
    Group {
        property_name: String,
        group: usize,
        direction: SortDirection,
    },
    #[serde(rename_all = "camelCase")]
    Event {
        property_name: String,
        direction: SortDirection,
    },
    #[serde(rename_all = "camelCase")]
    Custom {
        property_id: u64,
        direction: SortDirection,
    },
}

impl Into<common::query::PropertyRef> for PropertyRef {
    fn into(self) -> common::query::PropertyRef {
        match self {
            PropertyRef::Group {
                property_name,
                group,
            } => common::query::PropertyRef::Group(property_name, group),
            PropertyRef::Event { property_name } => {
                common::query::PropertyRef::Event(property_name)
            }
            PropertyRef::Custom { property_id } => common::query::PropertyRef::Custom(property_id),
        }
    }
}

impl Into<PropertyRef> for common::query::PropertyRef {
    fn into(self) -> PropertyRef {
        match self {
            common::query::PropertyRef::Group(property_name, group) => PropertyRef::Group {
                property_name,
                group,
            },
            common::query::PropertyRef::Event(property_name) => {
                PropertyRef::Event { property_name }
            }
            common::query::PropertyRef::Custom(property_id) => PropertyRef::Custom { property_id },
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum PropValueFilter {
    #[serde(rename_all = "camelCase")]
    Property {
        #[serde(flatten)]
        property: PropertyRef,
        operation: PropValueOperation,
        #[serde(skip_serializing_if = "Option::is_none")]
        value: Option<Vec<Value>>,
    },
    // #[serde(rename_all = "camelCase")]
    // Cohort { cohort_id: u64 },
    // #[serde(rename_all = "camelCase")]
    // Group { group_id: u64 },
}

impl Into<common::query::PropValueFilter> for PropValueFilter {
    fn into(self) -> common::query::PropValueFilter {
        match self {
            PropValueFilter::Property {
                property,
                operation,
                value,
            } => common::query::PropValueFilter::Property {
                property: property.to_owned().into(),
                operation: operation.to_owned().into(),
                value: match value {
                    None => None,
                    Some(v) => {
                        if v.is_empty() {
                            None
                        } else {
                            Some(v.iter().map(json_value_to_scalar).collect::<Vec<_>>())
                        }
                    }
                },
            },
        }
    }
}

impl Into<PropValueFilter> for common::query::PropValueFilter {
    fn into(self) -> PropValueFilter {
        match self {
            common::query::PropValueFilter::Property {
                property,
                operation,
                value,
            } => PropValueFilter::Property {
                property: property.to_owned().into(),
                operation: operation.to_owned().into(),
                value: match value {
                    None => None,
                    Some(v) => Some(v.iter().map(scalar_to_json_value).collect::<Vec<_>>()),
                },
            },
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ColumnType {
    Dimension,
    Metric,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Column {
    #[serde(flatten)]
    pub property: Option<PropertyRef>,
    #[serde(rename = "type")]
    pub typ: ColumnType,
    pub name: String,
    pub is_nullable: bool,
    pub data_type: DType,
    pub data: Vec<Value>,
    pub compare_values: Option<Vec<Value>>,
}

impl From<query::ColumnType> for ColumnType {
    fn from(value: query::ColumnType) -> Self {
        match value {
            query::ColumnType::Dimension => ColumnType::Dimension,
            query::ColumnType::Metric => ColumnType::Metric,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct JSONQueryResponse {
    columns: Vec<Column>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct JSONCompactQueryResponse(Vec<Vec<Value>>);

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
#[serde(untagged)]
pub enum QueryResponse {
    JSON(JSONQueryResponse),
    JSONCompact(JSONCompactQueryResponse),
}

impl QueryResponse {
    pub fn columns_to_json(columns: Vec<query::Column>) -> Result<Self> {
        let columns = columns
            .iter()
            .cloned()
            .map(|column| {
                let data = array_ref_to_json_values(&column.data);
                Column {
                    property: column.property.map(|p| p.into()),
                    typ: column.typ.into(),
                    name: column.name,
                    is_nullable: column.is_nullable,
                    data_type: column.data_type.into(),
                    data,
                    compare_values: None,
                }
            })
            .collect::<Vec<_>>();

        Ok(Self::JSON(JSONQueryResponse { columns }))
    }

    pub fn columns_to_json_compact(columns: Vec<query::Column>) -> Result<Self> {
        let data = columns
            .iter()
            .cloned()
            .map(|column| array_ref_to_json_values(&column.data))
            .collect::<Vec<_>>();

        Ok(Self::JSONCompact(JSONCompactQueryResponse(data)))
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct FunnelStepData {
    pub groups: Option<Vec<String>>,
    pub ts: i64,
    pub total: i64,
    pub conversion_ratio: Decimal,
    pub avg_time_to_convert: Decimal,
    pub avg_time_to_convert_from_start: Decimal,
    pub dropped_off: i64,
    pub drop_off_ratio: Decimal,
    pub time_to_convert: i64,
    pub time_to_convert_from_start: i64,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct FunnelStep {
    pub step: String,
    pub data: Vec<FunnelStepData>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct FunnelResponse {
    pub groups: Vec<String>,
    pub steps: Vec<FunnelStep>,
}

#[derive(Clone, Serialize, Deserialize, Default, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum EventGroupedFiltersGroupsCondition {
    And,
    #[default]
    Or,
}

#[derive(Clone, Serialize, Deserialize, Default, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum EventGroupedFilterGroupCondition {
    #[default]
    And,
    Or,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct EventGroupedFilterGroup {
    #[serde(default)]
    pub filters_condition: EventGroupedFilterGroupCondition,
    pub filters: Vec<PropValueFilter>,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct EventGroupedFilters {
    #[serde(default)]
    pub groups_condition: Option<EventGroupedFiltersGroupsCondition>,
    pub groups: Vec<EventGroupedFilterGroup>,
}


// queries


#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum QueryResponseFormat {
    Json,
    JsonCompact,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct QueryParams {
    pub format: Option<QueryResponseFormat>,
    pub timestamp: Option<i64>,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum QueryTime {
    Between {
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    },
    From {
        from: DateTime<Utc>,
    },
    Last {
        last: i64,
        unit: TimeIntervalUnit,
    },
}

impl Into<common::query::QueryTime> for QueryTime {
    fn into(self) -> common::query::QueryTime {
        match self {
            QueryTime::Between { from, to } => common::query::QueryTime::Between { from, to },
            QueryTime::From { from } => common::query::QueryTime::From(from),
            QueryTime::Last { last, unit } => common::query::QueryTime::Last {
                last,
                unit: unit.into(),
            },
        }
    }
}

impl Into<QueryTime> for common::query::QueryTime {
    fn into(self) -> QueryTime {
        match self {
            common::query::QueryTime::Between { from, to } => QueryTime::Between { from, to },
            common::query::QueryTime::From(from) => QueryTime::From { from },
            common::query::QueryTime::Last { last, unit } => QueryTime::Last {
                last,
                unit: unit.into(),
            },
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum TimeIntervalUnit {
    Hour,
    Day,
    Week,
    Month,
    Year,
}

impl Into<common::query::TimeIntervalUnit> for TimeIntervalUnit {
    fn into(self) -> common::query::TimeIntervalUnit {
        match self {
            TimeIntervalUnit::Hour => common::query::TimeIntervalUnit::Hour,
            TimeIntervalUnit::Day => common::query::TimeIntervalUnit::Day,
            TimeIntervalUnit::Week => common::query::TimeIntervalUnit::Week,
            TimeIntervalUnit::Month => common::query::TimeIntervalUnit::Month,
            TimeIntervalUnit::Year => common::query::TimeIntervalUnit::Year,
        }
    }
}

impl Into<TimeIntervalUnit> for common::query::TimeIntervalUnit {
    fn into(self) -> TimeIntervalUnit {
        match self {
            common::query::TimeIntervalUnit::Hour => TimeIntervalUnit::Hour,
            common::query::TimeIntervalUnit::Day => TimeIntervalUnit::Day,
            common::query::TimeIntervalUnit::Week => TimeIntervalUnit::Week,
            common::query::TimeIntervalUnit::Month => TimeIntervalUnit::Month,
            common::query::TimeIntervalUnit::Year => TimeIntervalUnit::Year,
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq, Hash)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum Breakdown {
    Property {
        #[serde(flatten)]
        property: PropertyRef,
    },
}

impl Into<common::query::Breakdown> for Breakdown {
    fn into(self) -> common::query::Breakdown {
        match self {
            Breakdown::Property { property } => {
                common::query::Breakdown::Property(property.to_owned().into())
            }
        }
    }
}

impl Into<Breakdown> for common::query::Breakdown {
    fn into(self) -> Breakdown {
        match self {
            common::query::Breakdown::Property(property) => Breakdown::Property {
                property: property.to_owned().into(),
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum AggregateFunction {
    /// count
    Count,
    /// sum
    Sum,
    /// min
    Min,
    /// max
    Max,
    /// avg
    Avg,
    /// Approximate aggregate function
    ApproxDistinct,
    /// array_agg
    ArrayAgg,
    /// Variance (Sample)
    Variance,
    /// Variance (Population)
    VariancePop,
    /// Standard Deviation (Sample)
    Stddev,
    /// Standard Deviation (Population)
    StddevPop,
    /// Covariance (Sample)
    Covariance,
    /// Covariance (Population)
    CovariancePop,
    /// Correlation
    Correlation,
    /// Approximate continuous percentile function
    ApproxPercentileCont,
    /// ApproxMedian
    ApproxMedian,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum DidEventAggregate {
    Count {
        operation: PropValueOperation,
        value: i64,
        time: SegmentTime,
    },
    RelativeCount {
        #[serde(flatten)]
        event: EventRef,
        operation: PropValueOperation,
        filters: Option<Vec<PropValueFilter>>,
        time: SegmentTime,
    },
    AggregateProperty {
        #[serde(flatten)]
        property: PropertyRef,
        aggregate: QueryAggregate,
        operation: PropValueOperation,
        value: Option<Value>,
        time: SegmentTime,
    },
    HistoricalCount {
        operation: PropValueOperation,
        value: u64,
        time: SegmentTime,
    },
}

impl Into<common::query::DidEventAggregate> for DidEventAggregate {
    fn into(self) -> common::query::DidEventAggregate {
        match self {
            DidEventAggregate::Count {
                operation,
                value,
                time,
            } => common::query::DidEventAggregate::Count {
                operation: operation.into(),
                value,
                time: time.into(),
            },
            DidEventAggregate::RelativeCount {
                event,
                operation,
                filters,
                time,
            } => common::query::DidEventAggregate::RelativeCount {
                event: event.into(),
                operation: operation.into(),
                filters: filters.map_or_else(
                    || None,
                    |v| {
                        if v.is_empty() {
                            None
                        } else {
                            Some(v.iter().map(|v| v.to_owned().into()).collect::<Vec<_>>())
                        }
                    },
                ),
                time: time.into(),
            },
            DidEventAggregate::AggregateProperty {
                property,
                aggregate,
                operation,
                value,
                time,
            } => common::query::DidEventAggregate::AggregateProperty {
                property: property.into(),
                aggregate: aggregate.into(),
                operation: operation.into(),
                value: value.map(|v| json_value_to_scalar(&v)),
                time: time.into(),
            },
            DidEventAggregate::HistoricalCount {
                operation,
                value,
                time,
            } => common::query::DidEventAggregate::HistoricalCount {
                operation: operation.into(),
                value,
                time: time.into(),
            },
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum SegmentTime {
    Between {
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    },
    From(DateTime<Utc>),
    Last {
        last: i64,
        unit: TimeIntervalUnit,
    },
    AfterFirstUse {
        within: i64,
        unit: TimeIntervalUnit,
    },
    WindowEach {
        unit: TimeIntervalUnit,
        n: i64,
    },
}

impl Into<common::query::SegmentTime> for SegmentTime {
    fn into(self) -> common::query::SegmentTime {
        match self {
            SegmentTime::Between { from, to } => common::query::SegmentTime::Between { from, to },
            SegmentTime::From(v) => common::query::SegmentTime::From(v),
            SegmentTime::Last { last: n, unit } => common::query::SegmentTime::Last {
                n,
                unit: unit.into(),
            },
            SegmentTime::AfterFirstUse { within, unit } => {
                common::query::SegmentTime::AfterFirstUse {
                    within,
                    unit: unit.into(),
                }
            }
            SegmentTime::WindowEach { unit, n } => common::query::SegmentTime::Each {
                n,
                unit: unit.into(),
            },
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum SegmentCondition {
    #[serde(rename_all = "camelCase")]
    HasPropertyValue {
        property: PropertyRef,
        operation: PropValueOperation,
        #[serde(skip_serializing_if = "Option::is_none")]
        value: Option<Vec<Value>>,
    },
    #[serde(rename_all = "camelCase")]
    HadPropertyValue {
        property: PropertyRef,
        operation: PropValueOperation,
        #[serde(skip_serializing_if = "Option::is_none")]
        value: Option<Vec<Value>>,
        time: SegmentTime,
    },
    #[serde(rename_all = "camelCase")]
    DidEvent {
        #[serde(flatten)]
        event: EventRef,
        filters: Option<Vec<PropValueFilter>>,
        aggregate: DidEventAggregate,
    },
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Segment {
    name: String,
    conditions: Vec<Vec<SegmentCondition>>,
}

impl Into<common::query::SegmentCondition> for SegmentCondition {
    fn into(self) -> common::query::SegmentCondition {
        match self {
            SegmentCondition::HasPropertyValue {
                property,
                operation,
                value,
            } => common::query::SegmentCondition::HasPropertyValue {
                property:property.into(),
                operation: operation.into(),
                value: match value {
                    Some(v) if !v.is_empty() => {
                        Some(v.iter().map(json_value_to_scalar).collect::<Vec<_>>())
                    }
                    _ => None,
                },
            },
            SegmentCondition::HadPropertyValue {
                property,
                operation,
                value,
                time,
            } => common::query::SegmentCondition::HadPropertyValue {
                property:property.into(),
                operation: operation.into(),
                value: match value {
                    Some(v) if !v.is_empty() => {
                        Some(v.iter().map(json_value_to_scalar).collect::<Vec<_>>())
                    }
                    _ => None,
                },
                time: time.into(),
            },
            SegmentCondition::DidEvent {
                event,
                filters,
                aggregate,
            } => common::query::SegmentCondition::DidEvent {
                event: event.into(),
                filters: filters.map_or_else(
                    || None,
                    |v| {
                        if v.is_empty() {
                            None
                        } else {
                            Some(v.iter().map(|v| v.to_owned().into()).collect::<Vec<_>>())
                        }
                    },
                ),
                aggregate: aggregate.into(),
            },
        }
    }
}

impl Into<SegmentTime> for common::query::SegmentTime {
    fn into(self) -> SegmentTime {
        match self {
            common::query::SegmentTime::Between { from, to } => {
                SegmentTime::Between { from, to }
            }
            common::query::SegmentTime::From(from) => SegmentTime::From(from),
            common::query::SegmentTime::Last { n, unit } => SegmentTime::Last {
                last: n,
                unit: unit.into(),
            },
            common::query::SegmentTime::AfterFirstUse { within, unit } => {
                SegmentTime::AfterFirstUse {
                    within,
                    unit: unit.into(),
                }
            }
            common::query::SegmentTime::Each { n, unit } => {
                SegmentTime::WindowEach {
                    unit: unit.into(),
                    n,
                }
            }
        }
    }
}

impl Into<DidEventAggregate> for common::query::DidEventAggregate {
    fn into(self) -> DidEventAggregate {
        match self {
            common::query::DidEventAggregate::Count {
                operation,
                value,
                time,
            } => DidEventAggregate::Count {
                operation: operation.into(),
                value,
                time: time.into(),
            },
            common::query::DidEventAggregate::RelativeCount {
                event,
                operation,
                filters,
                time,
            } => DidEventAggregate::RelativeCount {
                event: event.into(),
                operation: operation.into(),
                filters: filters.map_or_else(
                    || None,
                    |v| {
                        if v.is_empty() {
                            None
                        } else {
                            Some(v.iter().map(|v| v.to_owned().into()).collect::<Vec<_>>())
                        }
                    },
                ),
                time: time.into(),
            },
            common::query::DidEventAggregate::AggregateProperty {
                property,
                aggregate,
                operation,
                value,
                time,
            } => DidEventAggregate::AggregateProperty {
                property: property.into(),
                aggregate: aggregate.into(),
                operation: operation.into(),
                value: value.map(|v| scalar_to_json_value(&v)),
                time: time.into(),
            },
            common::query::DidEventAggregate::HistoricalCount {
                operation,
                value,
                time,
            } => DidEventAggregate::HistoricalCount {
                operation: operation.into(),
                value,
                time: time.into(),
            },
        }
    }
}

impl Into<SegmentCondition> for common::query::SegmentCondition {
    fn into(self) -> SegmentCondition {
        match self {
            common::query::SegmentCondition::HasPropertyValue {
                property,
                operation,
                value,
            } => SegmentCondition::HasPropertyValue {
                property:property.into(),
                operation: operation.into(),
                value: value.map_or_else(
                    || None,
                    |v| {
                        if v.is_empty() {
                            None
                        } else {
                            Some(v.iter().map(scalar_to_json_value).collect::<Vec<_>>())
                        }
                    },
                ),
            },
            common::query::SegmentCondition::HadPropertyValue {
                property,
                operation,
                value,
                time,
            } => SegmentCondition::HadPropertyValue {
                property:property.into(),
                operation: operation.into(),
                value: value.map_or_else(
                    || None,
                    |v| {
                        if v.is_empty() {
                            None
                        } else {
                            Some(v.iter().map(scalar_to_json_value).collect::<Vec<_>>())
                        }
                    },
                ),
                time: time.into(),
            },
            common::query::SegmentCondition::DidEvent {
                event,
                filters,
                aggregate,
            } => SegmentCondition::DidEvent {
                event: event.into(),
                filters: filters.map_or_else(
                    || None,
                    |v| {
                        if v.is_empty() {
                            None
                        } else {
                            Some(v.iter().map(|v| v.to_owned().into()).collect::<Vec<_>>())
                        }
                    },
                ),
                aggregate: aggregate.into(),
            },
        }
    }
}

impl Into<common::query::Segment> for Segment {
    fn into(self) -> common::query::Segment {
        common::query::Segment {
            name: self.name.clone(),
            conditions: self
                .conditions
                .iter()
                .map(|v| v.iter().map(|v| v.to_owned().into()).collect::<Vec<_>>())
                .collect::<Vec<_>>(),
        }
    }
}

impl Into<Segment> for common::query::Segment {
    fn into(self) -> Segment {
        Segment {
            name: self.name.clone(),
            conditions: self
                .conditions
                .iter()
                .map(|v| v.iter().map(|v| v.to_owned().into()).collect::<Vec<_>>())
                .collect::<Vec<_>>(),
        }
    }
}

impl Into<common::query::AggregateFunction> for &AggregateFunction {
    fn into(self) -> common::query::AggregateFunction {
        match self {
            AggregateFunction::Count => common::query::AggregateFunction::Count,
            AggregateFunction::Sum => common::query::AggregateFunction::Sum,
            AggregateFunction::Min => common::query::AggregateFunction::Min,
            AggregateFunction::Max => common::query::AggregateFunction::Max,
            AggregateFunction::Avg => common::query::AggregateFunction::Avg,
            _ => unimplemented!("unimplemented"),
        }
    }
}

impl Into<AggregateFunction> for common::query::AggregateFunction {
    fn into(self) -> AggregateFunction {
        match self {
            common::query::AggregateFunction::Count => AggregateFunction::Count,
            common::query::AggregateFunction::Sum => AggregateFunction::Sum,
            common::query::AggregateFunction::Min => AggregateFunction::Min,
            common::query::AggregateFunction::Max => AggregateFunction::Max,
            common::query::AggregateFunction::Avg => AggregateFunction::Avg,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PartitionedAggregateFunction {
    Sum,
    Avg,
    // Median,
    Count,
    Min,
    Max,
    // DistinctCount,
    // Percentile25,
    // Percentile75,
    // Percentile90,
    // Percentile99,
}

impl Into<common::query::PartitionedAggregateFunction> for &PartitionedAggregateFunction {
    fn into(self) -> common::query::PartitionedAggregateFunction {
        match self {
            PartitionedAggregateFunction::Count => {
                common::query::PartitionedAggregateFunction::Count
            }
            PartitionedAggregateFunction::Sum => common::query::PartitionedAggregateFunction::Sum,
            PartitionedAggregateFunction::Avg => common::query::PartitionedAggregateFunction::Avg,
            PartitionedAggregateFunction::Min => common::query::PartitionedAggregateFunction::Min,
            PartitionedAggregateFunction::Max => common::query::PartitionedAggregateFunction::Max,
        }
    }
}

impl Into<PartitionedAggregateFunction> for common::query::PartitionedAggregateFunction {
    fn into(self) -> PartitionedAggregateFunction {
        match self {
            common::query::PartitionedAggregateFunction::Count => {
                PartitionedAggregateFunction::Count
            }
            common::query::PartitionedAggregateFunction::Sum => PartitionedAggregateFunction::Sum,
            common::query::PartitionedAggregateFunction::Avg => PartitionedAggregateFunction::Avg,
            common::query::PartitionedAggregateFunction::Min => PartitionedAggregateFunction::Min,
            common::query::PartitionedAggregateFunction::Max => PartitionedAggregateFunction::Max,
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum QueryAggregate {
    Min,
    Max,
    Sum,
    Avg,
    Median,
    DistinctCount,
    Percentile25,
    Percentile75,
    Percentile90,
    Percentile99,
}

impl Into<common::query::QueryAggregate> for QueryAggregate {
    fn into(self) -> common::query::QueryAggregate {
        match self {
            QueryAggregate::Min => common::query::QueryAggregate::Min,
            QueryAggregate::Max => common::query::QueryAggregate::Max,
            QueryAggregate::Sum => common::query::QueryAggregate::Sum,
            QueryAggregate::Avg => common::query::QueryAggregate::Avg,
            QueryAggregate::Median => common::query::QueryAggregate::Median,
            QueryAggregate::DistinctCount => {
                common::query::QueryAggregate::DistinctCount
            }
            QueryAggregate::Percentile25 => {
                common::query::QueryAggregate::Percentile25th
            }
            QueryAggregate::Percentile75 => {
                common::query::QueryAggregate::Percentile75th
            }
            QueryAggregate::Percentile90 => {
                common::query::QueryAggregate::Percentile90th
            }
            QueryAggregate::Percentile99 => {
                common::query::QueryAggregate::Percentile99th
            }
        }
    }
}

impl Into<QueryAggregate> for common::query::QueryAggregate {
    fn into(self) -> QueryAggregate {
        match self {
            common::query::QueryAggregate::Min => QueryAggregate::Min,
            common::query::QueryAggregate::Max => QueryAggregate::Max,
            common::query::QueryAggregate::Sum => QueryAggregate::Sum,
            common::query::QueryAggregate::Avg => QueryAggregate::Avg,
            common::query::QueryAggregate::Median => QueryAggregate::Median,
            common::query::QueryAggregate::DistinctCount => {
                QueryAggregate::DistinctCount
            }
            common::query::QueryAggregate::Percentile25th => {
                QueryAggregate::Percentile25
            }
            common::query::QueryAggregate::Percentile75th => {
                QueryAggregate::Percentile75
            }
            common::query::QueryAggregate::Percentile90th => {
                QueryAggregate::Percentile90
            }
            common::query::QueryAggregate::Percentile99th => {
                QueryAggregate::Percentile99
            }
        }
    }
}


#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PropertyAndValue {
    #[serde(flatten)]
    property: PropertyRef,
    value: Value,
}

pub fn validate_event_property(
    md: &Arc<MetadataProvider>,
    project_id: u64,
    property: &PropertyRef,
    err_prefix: String,
) -> crate::Result<()> {
    match property {
        PropertyRef::Group {
            property_name,
            group,
        } => {
            md.group_properties[*group]
                .get_by_name(project_id, &property_name)
                .map_err(|err| PlatformError::BadRequest(format!("{err_prefix}: {err}")))?;
        }
        PropertyRef::Event { property_name } => {
            md.event_properties
                .get_by_name(project_id, &property_name)
                .map_err(|err| PlatformError::BadRequest(format!("{err_prefix}: {err}")))?;
        }
        _ => {
            return Err(PlatformError::Unimplemented(
                "invalid property type".to_string(),
            ));
        }
    }
    Ok(())
}

pub fn validate_event_filter_property(
    md: &Arc<MetadataProvider>,
    project_id: u64,
    property: &PropertyRef,
    operation: &PropValueOperation,
    value: &Option<Vec<Value>>,
    err_prefix: String,
) -> crate::Result<()> {
    let prop = match property {
        PropertyRef::Group {
            property_name,
            group,
        } => md.group_properties[*group]
            .get_by_name(project_id, &property_name)
            .map_err(|err| PlatformError::BadRequest(format!("{err_prefix}: {err}")))?,
        PropertyRef::Event { property_name } => md
            .event_properties
            .get_by_name(project_id, &property_name)
            .map_err(|err| PlatformError::BadRequest(format!("{err_prefix}: {err}")))?,
        _ => {
            return Err(PlatformError::Unimplemented(
                "invalid custom type".to_string(),
            ));
        }
    };
    let mut allow_empty_values = false;
    match operation {
        PropValueOperation::Gt
        | PropValueOperation::Gte
        | PropValueOperation::Lt
        | PropValueOperation::Lte => match prop.data_type {
            DType::Int8 | DType::Int16 | DType::Int64 | DType::Decimal | DType::Timestamp => {}
            _ => {
                return Err(PlatformError::BadRequest(format!(
                    "{err_prefix}property \"{}\" is not numeric, but operation is \"{:?}\"",
                    prop.name, operation
                )));
            }
        },
        PropValueOperation::True | PropValueOperation::False => {
            if prop.data_type != DType::Boolean {
                return Err(PlatformError::BadRequest(format!(
                    "{err_prefix}\"{}\" is not boolean, but operation is \"{:?}\"",
                    prop.name, operation
                )));
            }
            allow_empty_values = true;
        }
        PropValueOperation::Exists | PropValueOperation::Empty => {
            if !prop.nullable {
                return Err(PlatformError::BadRequest(format!(
                    "{err_prefix}property \"{}\" is not nullable, but operation is \"{:?}\"",
                    prop.name, operation
                )));
            }
            allow_empty_values = true;
        }
        PropValueOperation::Regex
        | PropValueOperation::Like
        | PropValueOperation::NotLike
        | PropValueOperation::NotRegex => {
            if prop.data_type != DType::String {
                return Err(PlatformError::BadRequest(format!(
                    "{err_prefix}property \"{}\" is not string, but operation is \"{:?}\"",
                    prop.name, operation
                )));
            }
        }
        _ => {}
    }
    match value {
        None => {
            if !prop.nullable {
                return Err(PlatformError::BadRequest(format!(
                    "{err_prefix}property \"{}\" is not nullable",
                    prop.name
                )));
            }
        }
        Some(values) => {
            if !allow_empty_values && values.is_empty() {
                return Err(PlatformError::BadRequest(format!(
                    "{err_prefix} values cannot be empty"
                )));
            }
            for (vid, value) in values.iter().enumerate() {
                match value {
                    Value::Null => {
                        if !prop.nullable {
                            return Err(PlatformError::BadRequest(format!(
                                "{err_prefix}property \"{}\" is not nullable, but value {vid} is \"{value:?}\"",
                                prop.name
                            )));
                        }
                    }
                    Value::Bool(_) => {
                        if prop.data_type != DType::Boolean {
                            return Err(PlatformError::BadRequest(format!(
                                "{err_prefix}property \"{}\" is not boolean, but value {vid} is \"{value:?}\"",
                                prop.name
                            )));
                        }
                    }
                    Value::Number(_) => match prop.data_type {
                        DType::Int8
                        | DType::Int16
                        | DType::Int32
                        | DType::Int64
                        | DType::Decimal
                        | DType::Timestamp => {}
                        _ => {
                            return Err(PlatformError::BadRequest(format!(
                                "{err_prefix}property \"{}\" is not numeric, but value {vid} is \"{value:?}\"",
                                prop.name
                            )));
                        }
                    },
                    Value::String(_) => {
                        if prop.data_type != DType::String {
                            return Err(PlatformError::BadRequest(format!(
                                "{err_prefix}property \"{}\" is not string, but value {vid} is \"{value:?}\"",
                                prop.name
                            )));
                        }
                    }
                    Value::Array(_) => {
                        return Err(PlatformError::Unimplemented(
                            "array value is unimplemented".to_string(),
                        ));
                    }
                    Value::Object(_) => {
                        return Err(PlatformError::Unimplemented(
                            "object value is unimplemented".to_string(),
                        ));
                    }
                }
            }
        }
    }
    Ok(())
}

pub(crate) fn validate_event(
    md: &Arc<MetadataProvider>,
    project_id: u64,
    event: &EventRef,
    event_id: usize,
    err_prefix: String,
) -> crate::Result<()> {
    match event {
        EventRef::Regular { event_name } => {
            md.events
                .get_by_name(project_id, &event_name)
                .map_err(|err| PlatformError::BadRequest(format!("event {event_id}: {err}")))?;
        }
        EventRef::Custom { event_id } => {
            md.custom_events
                .get_by_id(project_id, *event_id)
                .map_err(|err| PlatformError::BadRequest(format!("event {event_id}: {err}")))?;
        }
    }

    Ok(())
}

pub(crate) fn validate_event_filter(
    md: &Arc<MetadataProvider>,
    project_id: u64,
    filter: &PropValueFilter,
    filter_id: usize,
    err_prefix: String,
) -> crate::Result<()> {
    match filter {
        PropValueFilter::Property {
            property,
            operation,
            value,
        } => {
            validate_event_filter_property(
                md,
                project_id,
                property,
                operation,
                value,
                format!("{err_prefix}filter #{filter_id}"),
            )?;
        }
    }

    Ok(())
}
