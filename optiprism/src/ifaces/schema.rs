use crate::exprtree::error::{Error, Result};
use arrow::datatypes::DataType;
use chrono::{Date, Utc};
use datafusion::datasource::TableProvider;
use datafusion::logical_plan::DFSchema;
use datafusion::prelude::DataFrame;
use std::sync::Arc;
use arrow::datatypes;

pub mod event_fields {
    pub const CREATED_AT: &str = "created_at";
}

pub struct Schema {
    // todo usize or u64 for entity identifier?
    id: usize,
    project_id: u64,
    user: User,
    events: Vec<Event>,
}

impl Schema {
    pub fn get_event_by_name(&self, event_name: &str) -> Result<Event> {}
    pub fn get_event_property_by_name(&self, event_name: &str, property_name: &str) -> Result<Event> {}
    pub fn data_schema(&self) -> datatypes::Schema {}
}

trait SchemaProvider {
    fn get_event_by_name(&self, event_name: &str) -> Result<Event>
    fn get_event_property_by_name(&self, event_name: &str, property_name: &str) -> Result<Event>
    fn data_schema(&self) -> datatypes::Schema
}
pub struct User {
    properties: Vec<u64>,
}

pub struct UserProperty {
    id: u64,
    schema_id: u64,
    created_at: Date<Utc>,
    updated_at: Option<Date<Utc>>,
    created_by: u64,
    updated_by: u64,
    tags: Vec<String>,
    name: String,
    typ: DataType,
    nullable: bool,
    is_dictionary: bool,
    dictionary_type: Option<DataType>,
}

pub enum EventStatus {
    Enabled,
    Disabled,
}

pub struct EventProperty {
    id: u64,
    event_id: u64,
    created_at: Date<Utc>,
    updated_at: Option<Date<Utc>>,
    created_by: u64,
    updated_by: u64,
    tags: Vec<String>,
    name: String,
    typ: DataType,
    nullable: bool,
    is_dictionary: bool,
    dictionary_type: Option<DataType>,
}

pub struct Event {
    id: Option<u64>,
    created_at: Date<Utc>,
    updated_at: Option<Date<Utc>>,
    created_by: u64,
    updated_by: u64,
    project_id: u64,
    tags: Vec<String>,
    name: String,
    description: String,
    status: EventStatus,
    properties: Option<Vec<u64>>,
}

/*trait SchemaProvider {
    fn update_user(&self, user: &User) -> Result<User>;
    fn create_user_property(&self, prop: &UserProperty) -> Result<UserProperty>;
    fn update_user_property(&self, prop: &UserProperty) -> Result<UserProperty>;
    fn get_user_property_by_id(&self, id: u64) -> Result<UserProperty>;
    fn get_user_property_by_name(&self, name: String) -> Result<UserProperty>;
    fn delete_user_property(&self, id: u64) -> Result<()>;
    fn list_user_properties(&self) -> Result<Vec<UserProperty>>;
    fn user_property_table_provider(&self) -> Arc<dyn TableProvider>;

    fn create_event(&self, event: &Event) -> Result<Event>;
    fn update_event(&self, event: &Event) -> Result<Event>;
    fn get_event_by_id(&self, id: u64) -> Result<Event>;
    fn get_event_by_name(&self, project_id: u64, name: String) -> Result<Event>;
    fn delete_event(&self, id: u64) -> Result<()>;
    fn enable_event(&self, id: u64) -> Result<()>;
    fn disable_event(&self, id: u64) -> Result<()>;
    fn list_events(&self, project_id: u64) -> Result<Vec<Event>>;
    fn event_table_provider(&self) -> Arc<dyn TableProvider>;

    fn create_event_property(&self, prop: &EventProperty) -> Result<EventProperty>;
    fn update_event_property(&self, prop: &EventProperty) -> Result<EventProperty>;
    fn get_event_property_by_id(&self, event_id: u64, id: u64) -> Result<EventProperty>;
    fn get_event_property_by_name(&self, event_id: u64, name: String) -> Result<EventProperty>;
    fn delete_event_property(&self, event_id: u64, id: u64) -> Result<()>;
    fn list_event_properties(&self, event_id: u64) -> Result<Vec<EventProperty>>;
    fn event_property_table_provider(&self) -> Arc<dyn TableProvider>;
}
*/