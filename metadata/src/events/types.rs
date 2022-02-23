use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub trait IndexValues {
    fn status(&self) -> Status;
    fn project_id(&self) -> u64;
    fn name(&self) -> &str;
    fn display_name(&self) -> &Option<String>;
}

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

impl IndexValues for Event {
    fn status(&self) -> Status {
        self.status.clone()
    }

    fn project_id(&self) -> u64 {
        self.project_id
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn display_name(&self) -> &Option<String> {
        &self.display_name
    }
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

impl IndexValues for CreateEventRequest {
    fn status(&self) -> Status {
        self.status.clone()
    }

    fn project_id(&self) -> u64 {
        self.project_id
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn display_name(&self) -> &Option<String> {
        &self.display_name
    }
}

impl CreateEventRequest {
    pub fn into_event(self, id: u64, created_at: DateTime<Utc>) -> Event {
        Event {
            id,
            created_at,
            updated_at: None,
            created_by: self.created_by,
            updated_by: None,
            project_id: self.project_id,
            tags: self.tags,
            name: self.name,
            display_name: self.display_name,
            description: self.description,
            status: self.status,
            scope: self.scope,
            properties: self.properties,
            custom_properties: self.custom_properties,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct UpdateEventRequest {
    pub id: u64,
    pub created_by: u64,
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

impl IndexValues for UpdateEventRequest {
    fn status(&self) -> Status {
        self.status.clone()
    }

    fn project_id(&self) -> u64 {
        self.project_id
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn display_name(&self) -> &Option<String> {
        &self.display_name
    }
}

impl UpdateEventRequest {
    pub fn into_event(
        self,
        prev: Event,
        updated_at: DateTime<Utc>,
        updated_by: Option<u64>,
    ) -> Event {
        Event {
            id: self.id,
            created_at: prev.created_at,
            updated_at: Some(updated_at),
            created_by: self.created_by,
            updated_by,
            project_id: self.project_id,
            tags: self.tags,
            name: self.name,
            display_name: self.display_name,
            description: self.description,
            status: self.status,
            scope: self.scope,
            properties: self.properties,
            custom_properties: self.custom_properties,
        }
    }
}
