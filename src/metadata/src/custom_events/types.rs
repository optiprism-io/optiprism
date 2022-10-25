use chrono::DateTime;
use chrono::Utc;
use common::types::EventFilter;
use common::types::EventRef;
use common::types::OptionalProperty;
use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub enum Status {
    #[default]
    Enabled,
    Disabled,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Event {
    pub event: EventRef,
    pub filters: Option<Vec<EventFilter>>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
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

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct CreateCustomEventRequest {
    pub created_by: u64,
    pub tags: Option<Vec<String>>,
    pub name: String,
    pub description: Option<String>,
    pub status: Status,
    pub is_system: bool,
    pub events: Vec<Event>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct UpdateCustomEventRequest {
    pub updated_by: u64,
    pub tags: OptionalProperty<Option<Vec<String>>>,
    pub name: OptionalProperty<String>,
    pub description: OptionalProperty<Option<String>>,
    pub status: OptionalProperty<Status>,
    pub is_system: OptionalProperty<bool>,
    pub events: OptionalProperty<Vec<Event>>,
}
