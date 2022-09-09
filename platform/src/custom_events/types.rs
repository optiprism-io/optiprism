use crate::queries::types::{EventFilter, EventRef};
use crate::PlatformError;
use chrono::{DateTime, Utc};
use crate::error::Result;
use common::types::OptionalProperty;
use metadata::metadata::ListResponse;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum Status {
    Enabled,
    Disabled,
}

impl Into<Status> for metadata::custom_events::Status {
    fn into(self) -> Status {
        match self {
            metadata::custom_events::Status::Enabled => Status::Enabled,
            metadata::custom_events::Status::Disabled => Status::Disabled,
        }
    }
}

impl Into<metadata::custom_events::Status> for Status {
    fn into(self) -> metadata::custom_events::Status {
        match self {
            Status::Enabled => metadata::custom_events::Status::Enabled,
            Status::Disabled => metadata::custom_events::Status::Disabled,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Event {
    pub event: EventRef,
    pub filters: Option<Vec<EventFilter>>,
}

impl TryInto<metadata::custom_events::Event> for Event {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<metadata::custom_events::Event, Self::Error> {
        Ok(metadata::custom_events::Event {
            event: self.event.into(),
            filters: self
                .filters
                .map(|v| v.iter().map(|e| e.try_into()).collect())
                .transpose()?,
        })
    }
}

impl TryInto<Event> for metadata::custom_events::Event {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<Event, Self::Error> {
        Ok(Event {
            event: self.event.try_into()?,
            filters: self
                .filters
                .map(|v| v.iter().map(|e| e.to_owned().try_into()).collect())
                .transpose()?,
        })
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
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

impl TryInto<CustomEvent> for metadata::custom_events::CustomEvent {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<CustomEvent, Self::Error> {
        Ok(CustomEvent {
            id: self.id,
            created_at: self.created_at,
            updated_at: self.updated_at,
            created_by: self.created_by,
            updated_by: self.updated_by,
            project_id: self.project_id,
            tags: self.tags,
            name: self.name,
            description: self.description,
            status: self.status.into(),
            is_system: self.is_system,
            events: self.events.iter().map(|e|e.to_owned().try_into()).collect::<Result<_>>()?,
        })
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CreateCustomEventRequest {
    pub tags: Option<Vec<String>>,
    pub name: String,
    pub description: Option<String>,
    pub status: Status,
    pub is_system: bool,
    pub events: Vec<Event>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct UpdateCustomEventRequest {
    pub tags: OptionalProperty<Option<Vec<String>>>,
    pub name: OptionalProperty<String>,
    pub description: OptionalProperty<Option<String>>,
    pub status: OptionalProperty<Status>,
    pub is_system: OptionalProperty<bool>,
    pub events: OptionalProperty<Vec<Event>>,
}
