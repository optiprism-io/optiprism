use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum Status {
    Enabled,
    Disabled,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum Scope {
    System,
    User,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Event {
    pub id: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
    pub created_by: u64,
    pub updated_by: Option<u64>,
    pub project_id: u64,
    pub tags: Vec<String>,
    pub name: String,
    pub display_name: Option<String>,
    pub description: Option<String>,
    pub status: Status,
    pub scope: Scope,
    pub properties: Option<Vec<u64>>,
    pub custom_properties: Option<Vec<u64>>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct CreateEventRequest {
    pub created_by: u64,
    pub project_id: u64,
    pub tags: Vec<String>,
    pub name: String,
    pub display_name: Option<String>,
    pub description: Option<String>,
    pub status: Status,
    pub scope: Scope,
    pub properties: Option<Vec<u64>>,
    pub global_properties: Option<Vec<u64>>,
    pub custom_properties: Option<Vec<u64>>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct UpdateEventRequest {
    pub id: u64,
    pub updated_by: u64,
    pub project_id: u64,
    pub tags: Vec<String>,
    pub name: String,
    pub display_name: Option<String>,
    pub description: Option<String>,
    pub status: Status,
    pub scope: Scope,
    pub properties: Option<Vec<u64>>,
    pub global_properties: Option<Vec<u64>>,
    pub custom_properties: Option<Vec<u64>>,
}
