use chrono::DateTime;
use chrono::Utc;
use common::types::OptionalProperty;
use serde::Deserialize;
use serde::Serialize;

use crate::PlatformError;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum Status {
    Enabled,
    Disabled,
}

impl From<metadata::events::Status> for Status {
    fn from(s: metadata::events::Status) -> Self {
        match s {
            metadata::events::Status::Enabled => Status::Enabled,
            metadata::events::Status::Disabled => Status::Disabled,
        }
    }
}

impl From<Status> for metadata::events::Status {
    fn from(s: Status) -> Self {
        match s {
            Status::Enabled => metadata::events::Status::Enabled,
            Status::Disabled => metadata::events::Status::Disabled,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
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

impl TryInto<metadata::events::Event> for Event {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<metadata::events::Event, Self::Error> {
        Ok(metadata::events::Event {
            id: self.id,
            created_at: self.created_at,
            updated_at: self.updated_at,
            created_by: self.created_by,
            updated_by: self.updated_by,
            project_id: self.project_id,
            tags: self.tags,
            name: self.name,
            display_name: self.display_name,
            description: self.description,
            status: self.status.into(),
            is_system: self.is_system,
            properties: self.properties,
            custom_properties: self.custom_properties,
        })
    }
}

impl TryInto<Event> for metadata::events::Event {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<Event, Self::Error> {
        Ok(Event {
            id: self.id,
            created_at: self.created_at,
            updated_at: self.updated_at,
            created_by: self.created_by,
            updated_by: self.updated_by,
            project_id: self.project_id,
            tags: self.tags,
            name: self.name,
            display_name: self.display_name,
            description: self.description,
            status: self.status.into(),
            is_system: self.is_system,
            properties: self.properties,
            custom_properties: self.custom_properties,
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateEventRequest {
    pub tags: Option<Vec<String>>,
    pub name: String,
    pub display_name: Option<String>,
    pub description: Option<String>,
    pub status: Status,
    pub is_system: bool,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateEventRequest {
    #[serde(default, skip_serializing_if = "OptionalProperty::is_none")]
    pub tags: OptionalProperty<Option<Vec<String>>>,
    #[serde(default, skip_serializing_if = "OptionalProperty::is_none")]
    pub display_name: OptionalProperty<Option<String>>,
    #[serde(default, skip_serializing_if = "OptionalProperty::is_none")]
    pub description: OptionalProperty<Option<String>>,
    #[serde(default, skip_serializing_if = "OptionalProperty::is_none")]
    pub status: OptionalProperty<Status>,
}
