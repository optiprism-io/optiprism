use chrono::{DateTime, Utc};
use common::types::OptionalProperty;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum Status {
    Enabled,
    Disabled,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Event {
    pub id: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
    pub created_by: u64,
    pub updated_by: Option<u64>,
    pub project_id: u64,
    pub tags: Option<Vec<String>>,
    pub name: String,
    pub display_name: Option<String>,
    pub description: Option<String>,
    pub status: Status,
    pub is_system: bool,
    pub properties: Option<Vec<u64>>,
    pub custom_properties: Option<Vec<u64>>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct CreateEventRequest {
    pub created_by: u64,
    pub tags: Option<Vec<String>>,
    pub name: String,
    pub display_name: Option<String>,
    pub description: Option<String>,
    pub status: Status,
    pub is_system: bool,
    pub properties: Option<Vec<u64>>,
    pub custom_properties: Option<Vec<u64>>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default)]
pub struct UpdateEventRequest {
    pub updated_by: u64,
    pub tags: OptionalProperty<Option<Vec<String>>>,
    pub name: OptionalProperty<String>,
    pub display_name: OptionalProperty<Option<String>>,
    pub description: OptionalProperty<Option<String>>,
    pub status: OptionalProperty<Status>,
    pub is_system: OptionalProperty<bool>,
    pub properties: OptionalProperty<Option<Vec<u64>>>,
    pub custom_properties: OptionalProperty<Option<Vec<u64>>>,
}
