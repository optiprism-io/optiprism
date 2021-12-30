use crate::error::{Result, StoreError};
use chrono::{Date, DateTime, Utc};
use datafusion::arrow::datatypes::{DataType, Schema as ArrowSchema};
use datafusion::datasource::TableProvider;
use datafusion::logical_plan::DFSchema;
use datafusion::prelude::DataFrame;
use std::sync::Arc;

pub mod event_fields {
    pub const EVENT_NAME: &str = "event_name";
    pub const CREATED_AT: &str = "created_at";
    pub const USER_ID: &str = "user_id";
}

pub struct MockSchema {
    pub get_event_by_name: Option<fn(event_name: &str) -> Result<Event>>,
    pub get_event_property_by_name:
        Option<fn(event_name: &str, property_name: &str) -> Result<EventProperty>>,
    pub get_event_custom_property_by_name:
        Option<fn(event_name: &str, property_name: &str) -> Result<EventCustomProperty>>,
    pub get_user_property_by_name: Option<fn(property_name: &str) -> Result<UserProperty>>,
    pub get_user_custom_property_by_name:
        Option<fn(property_name: &str) -> Result<UserCustomProperty>>,
    pub schema: Option<fn() -> ArrowSchema>,
}

impl MockSchema {
    pub fn new() -> Self {
        Self {
            get_event_by_name: None,
            get_event_property_by_name: None,
            get_event_custom_property_by_name: None,
            get_user_property_by_name: None,
            get_user_custom_property_by_name: None,
            schema: None,
        }
    }
}

impl SchemaProvider for MockSchema {
    fn get_event_by_name(&self, event_name: &str) -> Result<Event> {
        self.get_event_by_name.unwrap()(event_name)
    }

    fn get_event_property_by_name(
        &self,
        event_name: &str,
        property_name: &str,
    ) -> Result<EventProperty> {
        self.get_event_property_by_name.unwrap()(event_name, property_name)
    }

    fn get_event_custom_property_by_name(
        &self,
        event_name: &str,
        property_name: &str,
    ) -> Result<EventCustomProperty> {
        self.get_event_custom_property_by_name.unwrap()(event_name, property_name)
    }

    fn get_user_property_by_name(&self, property_name: &str) -> Result<UserProperty> {
        self.get_user_property_by_name.unwrap()(property_name)
    }

    fn get_user_custom_property_by_name(&self, property_name: &str) -> Result<UserCustomProperty> {
        self.get_user_custom_property_by_name.unwrap()(property_name)
    }

    fn schema(&self) -> ArrowSchema {
        self.schema.unwrap()()
    }
}

pub struct Schema {
    id: u64,
    project_id: u64,
    user: User,
    events: Vec<Event>,
}

pub struct CustomEvent {}

pub struct EventCustomProperty {}

pub trait SchemaProvider {
    fn get_event_by_name(&self, event_name: &str) -> Result<Event>;
    fn get_event_property_by_name(
        &self,
        event_name: &str,
        property_name: &str,
    ) -> Result<EventProperty>;
    fn get_event_custom_property_by_name(
        &self,
        event_name: &str,
        property_name: &str,
    ) -> Result<EventCustomProperty>;
    fn get_user_property_by_name(&self, property_name: &str) -> Result<UserProperty>;
    fn get_user_custom_property_by_name(&self, property_name: &str) -> Result<UserCustomProperty>;
    fn schema(&self) -> ArrowSchema;
}

pub struct User {
    properties: Vec<u64>,
}

#[derive(Clone)]
pub enum DBCol {
    Named(String),
    Order(usize),
}

pub struct UserProperty {
    pub id: u64,
    pub schema_id: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
    pub created_by: u64,
    pub updated_by: u64,
    pub is_system: bool,
    pub tags: Vec<String>,
    pub name: String,
    pub description: String,
    pub typ: DataType,
    pub db_col: DBCol,
    pub nullable: bool,
    pub is_array: bool,
    pub is_dictionary: bool,
    pub dictionary_type: Option<DataType>,
}

pub struct UserCustomProperty {
    pub id: u64,
    pub schema_id: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
    pub created_by: u64,
    pub updated_by: u64,
    pub is_system: bool,
    pub tags: Vec<String>,
    pub name: String,
    pub typ: DataType,
    pub nullable: bool,
    pub is_dictionary: bool,
    pub dictionary_type: Option<DataType>,
}

pub enum EventStatus {
    Enabled,
    Disabled,
}

pub enum EventPropertyStatus {
    Enabled,
    Disabled,
}

pub struct EventProperty {
    pub id: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
    pub created_by: u64,
    pub updated_by: u64,
    pub is_system: bool,
    pub is_global: bool,
    pub tags: Vec<String>,
    pub name: String,
    pub description: String,
    pub display_name: String,
    pub typ: DataType,
    pub db_col: DBCol,
    pub status: EventPropertyStatus,
    pub nullable: bool, // this also defines whether property is required or not
    pub is_array: bool,
    pub is_dictionary: bool,
    pub dictionary_type: Option<DataType>,
}

fn datatype_db_name(dt: &DataType) -> String {
    match dt {
        DataType::Null => panic!("unimplemented"),
        DataType::Boolean => "bool".to_string(),
        DataType::Int8 => "int8".to_string(),
        DataType::Int16 => "int16".to_string(),
        DataType::Int32 => "int32".to_string(),
        DataType::Int64 => "int64".to_string(),
        DataType::UInt8 => "uint8".to_string(),
        DataType::UInt16 => "uint16".to_string(),
        DataType::UInt32 => "uint32".to_string(),
        DataType::UInt64 => "uint64".to_string(),
        DataType::Float16 => "float16".to_string(),
        DataType::Float32 => "float32".to_string(),
        DataType::Float64 => "float64".to_string(),
        DataType::Timestamp(_, _) => "timestamp".to_string(),
        DataType::Date32 => "date32".to_string(),
        DataType::Date64 => "date64".to_string(),
        DataType::Time32(_) => "time32".to_string(),
        DataType::Time64(_) => "time64".to_string(),
        DataType::Duration(_) => "duration".to_string(),
        DataType::Interval(_) => "interval".to_string(),
        DataType::Binary => "binary".to_string(),
        DataType::FixedSizeBinary(_) => "fixed_size_binary".to_string(),
        DataType::LargeBinary => "large_binary".to_string(),
        DataType::Utf8 => "utf8".to_string(),
        DataType::LargeUtf8 => "large_utf8".to_string(),
        DataType::List(f) => format!("list_{}", datatype_db_name(f.data_type())),
        DataType::FixedSizeList(f, _) => {
            format!("fixed_size_list_{}", datatype_db_name(f.data_type()))
        }
        DataType::LargeList(f) => format!("large_list_{}", datatype_db_name(f.data_type())),
        DataType::Struct(_) => "struct".to_string(),
        DataType::Union(_) => "union".to_string(),
        DataType::Dictionary(_, _) => "dictionary".to_string(),
        DataType::Decimal(_, _) => "decimal".to_string(),
        DataType::Map(_, _) => unimplemented!(),
    }
}

fn db_col_name(db_col: &DBCol, typ: &DataType, dictionary_type: &Option<DataType>) -> String {
    match db_col {
        DBCol::Named(name) => name.clone(),
        DBCol::Order(order) => match dictionary_type {
            None => format!("{}_{}", *order, datatype_db_name(typ)),
            Some(t) => format!("{}_{}", *order, datatype_db_name(t)),
        },
    }
}

impl UserProperty {
    pub fn db_col_name(&self) -> String {
        db_col_name(&self.db_col, &self.typ, &self.dictionary_type)
    }
}

impl EventProperty {
    pub fn db_col_name(&self) -> String {
        db_col_name(&self.db_col, &self.typ, &self.dictionary_type)
    }
}

pub struct Event {
    pub id: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
    pub created_by: u64,
    pub updated_by: u64,
    pub project_id: u64,
    pub is_system: bool,
    pub tags: Vec<String>,
    pub name: String,
    pub description: String,
    pub status: EventStatus,
    pub properties: Option<Vec<u64>>,
}
