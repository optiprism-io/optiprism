use crate::OptionalProperty;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use datafusion_common::ScalarValue;
use crate::types::{EventRef, PropertyRef, PropValueOperation};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Status {
    Enabled,
    Disabled,
}

#[derive(Clone, Debug)]
pub enum EventFilter {
    Property {
        property: PropertyRef,
        operation: PropValueOperation,
        value: Option<Vec<ScalarValue>>,
    },
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum EventType {
    Regular,
    Custom,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Event {
    pub id: u64,
    pub typ: EventType,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct CustomEvent {
    pub id: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
    pub created_by: u64,
    pub updated_by: Option<u64>,
    pub project_id: u64,
    pub tags: Option<Vec<String>>,
    pub name: String,
    pub description: Option<String>,
    pub status: Status,
    pub is_system: bool,
    pub events: Vec<Event>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct CreateCustomEventRequest {
    pub created_by: u64,
    pub tags: Option<Vec<String>>,
    pub name: String,
    pub description: Option<String>,
    pub status: Status,
    pub is_system: bool,
    pub events: Vec<Event>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default)]
pub struct UpdateCustomEventRequest {
    pub updated_by: u64,
    pub tags: OptionalProperty<Option<Vec<String>>>,
    pub name: OptionalProperty<String>,
    pub description: OptionalProperty<Option<String>>,
    pub status: OptionalProperty<Status>,
    pub is_system: OptionalProperty<bool>,
    pub events: OptionalProperty<Vec<Event>>,
}
