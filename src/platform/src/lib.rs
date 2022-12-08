extern crate core;

pub mod accounts;
pub mod auth;
pub mod context;
pub mod custom_events;
pub mod dashboards;
pub mod error;
pub mod event_records;
pub mod events;
pub mod group_records;
pub mod http;
pub mod properties;
pub mod queries;
pub mod reports;
pub mod stub;
pub mod datatype;

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
use arrow::array::UInt16Array;
use arrow::array::UInt32Array;
use arrow::array::UInt64Array;
use arrow::array::UInt8Array;
use common::DECIMAL_PRECISION;
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
use crate::datatype::DataType;

pub struct PlatformProvider {
    pub events: Arc<dyn events::Provider>,
    pub custom_events: Arc<dyn custom_events::Provider>,
    pub event_properties: Arc<dyn properties::Provider>,
    pub user_properties: Arc<dyn properties::Provider>,
    pub accounts: Arc<dyn accounts::Provider>,
    pub auth: Arc<dyn auth::Provider>,
    pub query: Arc<dyn queries::Provider>,
    pub dashboards: Arc<dyn dashboards::Provider>,
    pub reports: Arc<dyn reports::Provider>,
    pub event_records: Arc<dyn event_records::Provider>,
    pub group_records: Arc<dyn group_records::Provider>,
}

impl PlatformProvider {
    pub fn new(
        md: Arc<MetadataProvider>,
        query_prov: Arc<query::ProviderImpl>,
        auth_cfg: auth::Config,
    ) -> Self {
        Self {
            events: Arc::new(events::ProviderImpl::new(md.events.clone())),
            custom_events: Arc::new(custom_events::ProviderImpl::new(md.custom_events.clone())),
            event_properties: Arc::new(properties::ProviderImpl::new_event(
                md.event_properties.clone(),
            )),
            user_properties: Arc::new(properties::ProviderImpl::new_user(
                md.user_properties.clone(),
            )),
            accounts: Arc::new(accounts::ProviderImpl::new(md.accounts.clone())),
            auth: Arc::new(auth::ProviderImpl::new(md.accounts.clone(), auth_cfg)),
            query: Arc::new(queries::ProviderImpl::new(query_prov)),
            dashboards: Arc::new(stub::Dashboards {}),
            reports: Arc::new(stub::Reports {}),
            event_records: Arc::new(stub::EventRecords {}),
            group_records: Arc::new(stub::GroupRecords {}),
        }
    }

    pub fn new_stub() -> Self {
        PlatformProvider {
            events: Arc::new(stub::Events {}),
            custom_events: Arc::new(stub::CustomEvents {}),
            event_properties: Arc::new(stub::Properties {}),
            user_properties: Arc::new(stub::Properties {}),
            accounts: Arc::new(stub::Accounts {}),
            auth: Arc::new(stub::Auth {}),
            query: Arc::new(stub::Queries {}),
            dashboards: Arc::new(stub::Dashboards {}),
            reports: Arc::new(stub::Reports {}),
            event_records: Arc::new(stub::EventRecords {}),
            group_records: Arc::new(stub::GroupRecords {}),
        }
    }
}

#[macro_export]
macro_rules! arr_to_json_values {
    ($array_ref:expr,$array_type:ident) => {{
        let arr = $array_ref.as_any().downcast_ref::<$array_type>().unwrap();
        Ok(arr.iter().map(|value| json!(value)).collect())
    }};
}

pub fn array_ref_to_json_values(arr: &ArrayRef) -> Result<Vec<Value>> {
    match arr.data_type() {
        arrow::datatypes::DataType::Int8 => arr_to_json_values!(arr, Int8Array),
        arrow::datatypes::DataType::Int16 => arr_to_json_values!(arr, Int16Array),
        arrow::datatypes::DataType::Int32 => arr_to_json_values!(arr, Int32Array),
        arrow::datatypes::DataType::Int64 => arr_to_json_values!(arr, Int64Array),
        arrow::datatypes::DataType::UInt8 => arr_to_json_values!(arr, UInt8Array),
        arrow::datatypes::DataType::UInt16 => arr_to_json_values!(arr, UInt16Array),
        arrow::datatypes::DataType::UInt32 => arr_to_json_values!(arr, UInt32Array),
        arrow::datatypes::DataType::UInt64 => arr_to_json_values!(arr, UInt64Array),
        arrow::datatypes::DataType::Float32 => arr_to_json_values!(arr, Float32Array),
        arrow::datatypes::DataType::Float64 => arr_to_json_values!(arr, Float64Array),
        arrow::datatypes::DataType::Boolean => arr_to_json_values!(arr, BooleanArray),
        arrow::datatypes::DataType::Utf8 => arr_to_json_values!(arr, StringArray),
        arrow::datatypes::DataType::Decimal128(_, s) => {
            let arr = arr.as_any().downcast_ref::<Decimal128Array>().unwrap();
            arr.iter()
                .map(|value| match value {
                    None => Ok(Value::Null),
                    Some(v) => {
                        let d = match Decimal::try_new(v as i64, *s as u32) {
                            Ok(v) => v,
                            Err(err) => return Err(err.into()),
                        };
                        let d_f = match d.to_f64() {
                            None => {
                                return Err(PlatformError::Internal(
                                    "can't convert decimal to f64".to_string(),
                                ));
                            }
                            Some(v) => v,
                        };
                        let n = match Number::from_f64(d_f) {
                            None => {
                                return Err(PlatformError::Internal(
                                    "can't make json number from f64".to_string(),
                                ));
                            }
                            Some(v) => v,
                        };
                        Ok(Value::Number(n))
                    }
                })
                .collect::<Result<_>>()
        }
        _ => unimplemented!(),
    }
}

#[derive(Serialize, Deserialize, Default, Debug, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ResponseMetadata {
    pub next: Option<String>,
}

#[derive(Serialize, Deserialize, Default, Debug, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ListResponse<T>
where T: Debug
{
    pub data: Vec<T>,
    pub meta: ResponseMetadata,
}

impl<A, B> TryInto<ListResponse<A>> for metadata::metadata::ListResponse<B>
where
    A: Debug,
    B: TryInto<A> + Clone + Debug,
    PlatformError: std::convert::From<<B as TryInto<A>>::Error>,
{
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<ListResponse<A>, Self::Error> {
        let data = self
            .data
            .into_iter()
            .map(|v| v.try_into())
            .collect::<std::result::Result<Vec<A>, B::Error>>()?;
        let meta = ResponseMetadata {
            next: self.meta.next,
        };
        Ok(ListResponse { data, meta })
    }
}

pub fn json_value_to_scalar(v: &Value) -> Result<ScalarValue> {
    match v {
        Value::Bool(v) => Ok(ScalarValue::Boolean(Some(*v))),
        Value::Number(n) => {
            let dec = Decimal::try_from(n.as_f64().unwrap())?;
            Ok(ScalarValue::Decimal128(
                Some(dec.mantissa()),
                DECIMAL_PRECISION,
                dec.scale() as i8,
            ))
        }
        Value::String(v) => Ok(ScalarValue::Utf8(Some(v.to_string()))),
        _ => Err(PlatformError::BadRequest("unexpected value".to_string())),
    }
}

pub fn scalar_to_json_value(v: &ScalarValue) -> Result<Value> {
    match v {
        ScalarValue::Decimal128(None, _, _) => Ok(Value::Null),
        ScalarValue::Boolean(None) => Ok(Value::Null),
        ScalarValue::Utf8(None) => Ok(Value::Null),
        ScalarValue::Decimal128(Some(v), _p, s) => Ok(Value::Number(
            Number::from_f64(Decimal::new(*v as i64, *s as u32).to_f64().unwrap()).unwrap(),
        )),
        ScalarValue::Boolean(Some(v)) => Ok(Value::Bool(*v)),
        ScalarValue::Utf8(Some(v)) => Ok(Value::String(v.to_owned())),
        _ => Err(PlatformError::BadRequest("unexpected value".to_string())),
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
    ArrAll,
    ArrAny,
    ArrNone,
    Regex,
    Like,
    NotLike,
    NotRegex,
}

impl TryInto<common::types::PropValueOperation> for PropValueOperation {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<common::types::PropValueOperation, Self::Error> {
        Ok(match self {
            PropValueOperation::Eq => common::types::PropValueOperation::Eq,
            PropValueOperation::Neq => common::types::PropValueOperation::Neq,
            PropValueOperation::Gt => common::types::PropValueOperation::Gt,
            PropValueOperation::Gte => common::types::PropValueOperation::Gte,
            PropValueOperation::Lt => common::types::PropValueOperation::Lt,
            PropValueOperation::Lte => common::types::PropValueOperation::Lte,
            PropValueOperation::True => common::types::PropValueOperation::True,
            PropValueOperation::False => common::types::PropValueOperation::False,
            PropValueOperation::Exists => common::types::PropValueOperation::Exists,
            PropValueOperation::Empty => common::types::PropValueOperation::Empty,
            PropValueOperation::ArrAll => common::types::PropValueOperation::ArrAll,
            PropValueOperation::ArrAny => common::types::PropValueOperation::ArrAny,
            PropValueOperation::ArrNone => common::types::PropValueOperation::ArrNone,
            PropValueOperation::Regex => common::types::PropValueOperation::Regex,
            PropValueOperation::Like => common::types::PropValueOperation::Like,
            PropValueOperation::NotLike => common::types::PropValueOperation::NotLike,
            PropValueOperation::NotRegex => common::types::PropValueOperation::NotRegex,
        })
    }
}

impl TryInto<PropValueOperation> for common::types::PropValueOperation {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<PropValueOperation, Self::Error> {
        Ok(match self {
            common::types::PropValueOperation::Eq => PropValueOperation::Eq,
            common::types::PropValueOperation::Neq => PropValueOperation::Neq,
            common::types::PropValueOperation::Gt => PropValueOperation::Gt,
            common::types::PropValueOperation::Gte => PropValueOperation::Gte,
            common::types::PropValueOperation::Lt => PropValueOperation::Lt,
            common::types::PropValueOperation::Lte => PropValueOperation::Lte,
            common::types::PropValueOperation::True => PropValueOperation::True,
            common::types::PropValueOperation::False => PropValueOperation::False,
            common::types::PropValueOperation::Exists => PropValueOperation::Exists,
            common::types::PropValueOperation::Empty => PropValueOperation::Empty,
            common::types::PropValueOperation::ArrAll => PropValueOperation::ArrAll,
            common::types::PropValueOperation::ArrAny => PropValueOperation::ArrAny,
            common::types::PropValueOperation::ArrNone => PropValueOperation::ArrNone,
            common::types::PropValueOperation::Regex => PropValueOperation::Regex,
            common::types::PropValueOperation::Like => PropValueOperation::Like,
            common::types::PropValueOperation::NotLike => PropValueOperation::NotLike,
            common::types::PropValueOperation::NotRegex => PropValueOperation::NotRegex,
        })
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
            EventRef::Custom { event_id } => format!("{}_custom_{}", event_id, idx),
        }
    }
}

impl From<EventRef> for common::types::EventRef {
    fn from(e: EventRef) -> Self {
        match e {
            EventRef::Regular { event_name } => common::types::EventRef::RegularName(event_name),
            EventRef::Custom { event_id } => common::types::EventRef::Custom(event_id),
        }
    }
}

impl TryInto<EventRef> for common::types::EventRef {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<EventRef, Self::Error> {
        Ok(match self {
            common::types::EventRef::RegularName(name) => EventRef::Regular { event_name: name },
            common::types::EventRef::Regular(_id) => unimplemented!(),
            common::types::EventRef::Custom(id) => EventRef::Custom { event_id: id },
        })
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(tag = "propertyType", rename_all = "camelCase")]
pub enum PropertyRef {
    #[serde(rename_all = "camelCase")]
    User { property_name: String },
    #[serde(rename_all = "camelCase")]
    Event { property_name: String },
    #[serde(rename_all = "camelCase")]
    Custom { property_id: u64 },
}

impl TryInto<common::types::PropertyRef> for PropertyRef {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<common::types::PropertyRef, Self::Error> {
        Ok(match self {
            PropertyRef::User { property_name } => common::types::PropertyRef::User(property_name),
            PropertyRef::Event { property_name } => {
                common::types::PropertyRef::Event(property_name)
            }
            PropertyRef::Custom { property_id } => common::types::PropertyRef::Custom(property_id),
        })
    }
}

impl TryInto<PropertyRef> for common::types::PropertyRef {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<PropertyRef, Self::Error> {
        Ok(match self {
            common::types::PropertyRef::User(property_name) => PropertyRef::User { property_name },
            common::types::PropertyRef::Event(property_name) => {
                PropertyRef::Event { property_name }
            }
            common::types::PropertyRef::Custom(property_id) => PropertyRef::Custom { property_id },
        })
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum EventFilter {
    #[serde(rename_all = "camelCase")]
    Property {
        #[serde(flatten)]
        property: PropertyRef,
        operation: PropValueOperation,
        #[serde(skip_serializing_if = "Option::is_none")]
        value: Option<Vec<Value>>,
    },
    #[serde(rename_all = "camelCase")]
    Cohort { cohort_id: u64 },
    #[serde(rename_all = "camelCase")]
    Group { group_id: u64 },
}

impl TryInto<common::types::EventFilter> for &EventFilter {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<common::types::EventFilter, Self::Error> {
        Ok(match self {
            EventFilter::Property {
                property,
                operation,
                value,
            } => common::types::EventFilter::Property {
                property: property.to_owned().try_into()?,
                operation: operation.to_owned().try_into()?,
                value: match value {
                    None => None,
                    Some(v) => Some(v.iter().map(json_value_to_scalar).collect::<Result<_>>()?),
                },
            },
            _ => todo!(),
        })
    }
}

impl TryInto<EventFilter> for common::types::EventFilter {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<EventFilter, Self::Error> {
        Ok(match self {
            common::types::EventFilter::Property {
                property,
                operation,
                value,
            } => EventFilter::Property {
                property: property.try_into()?,
                operation: operation.try_into()?,
                value: match value {
                    None => None,
                    Some(v) => Some(v.iter().map(scalar_to_json_value).collect::<Result<_>>()?),
                },
            },
        })
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ColumnType {
    Dimension,
    Metric,
    MetricValue,
    FunnelMetricValue,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Column {
    #[serde(rename = "type")]
    pub typ: ColumnType,
    pub name: String,
    pub is_nullable: bool,
    pub data_type: DataType,
    pub step: Option<usize>,
    pub data: Vec<Value>,
    pub compare_values: Option<Vec<Value>>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct DataTable {
    columns: Vec<Column>,
}

impl DataTable {
    pub fn new(columns: Vec<Column>) -> Self {
        Self { columns }
    }
}

impl TryFrom<query::DataTable> for DataTable {
    type Error = PlatformError;

    fn try_from(value: query::DataTable) -> std::result::Result<Self, Self::Error> {
        let cols = value
            .columns
            .iter()
            .cloned()
            .map(|column| match array_ref_to_json_values(&column.data) {
                Ok(data) => Ok(Column {
                    typ: ColumnType::Dimension,
                    name: column.name,
                    is_nullable: column.is_nullable,
                    data_type: column.data_type.try_into()?,
                    data,
                    step: None,
                    compare_values: None,
                }),
                Err(err) => Err(err),
            })
            .collect::<Result<_>>()?;

        Ok(DataTable::new(cols))
    }
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
    pub filters: Vec<EventFilter>,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct EventGroupedFilters {
    #[serde(default)]
    pub groups_condition: Option<EventGroupedFiltersGroupsCondition>,
    pub groups: Vec<EventGroupedFilterGroup>,
}
