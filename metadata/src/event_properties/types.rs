use arrow::datatypes::DataType;
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
pub struct EventProperty {
    pub id: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
    pub created_by: u64,
    pub updated_by: Option<u64>,
    pub project_id: u64,
    pub tags: Vec<String>,
    pub name: String,
    pub description: String,
    pub display_name: Option<String>,
    pub typ: DataType,
    pub col_id: u64,
    pub status: Status,
    pub scope: Scope,
    pub nullable: bool,
    // this also defines whether property is required or not
    pub is_array: bool,
    pub is_dictionary: bool,
    pub dictionary_type: Option<DataType>,
}

impl IndexValues for EventProperty {
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
pub struct CreateEventPropertyRequest {
    pub created_by: u64,
    pub project_id: u64,
    pub tags: Vec<String>,
    pub name: String,
    pub description: String,
    pub display_name: Option<String>,
    pub typ: DataType,
    pub status: Status,
    pub scope: Scope,
    pub nullable: bool,
    // this also defines whether property is required or not
    pub is_array: bool,
    pub is_dictionary: bool,
    pub dictionary_type: Option<DataType>,
}

impl IndexValues for CreateEventPropertyRequest {
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

impl CreateEventPropertyRequest {
    pub fn into_event_property(self, id: u64, col_id: u64, created_at: DateTime<Utc>) -> EventProperty {
        EventProperty {
            id,
            created_at,
            updated_at: None,
            created_by: self.created_by,
            updated_by: None,
            project_id: self.project_id,
            tags: self.tags,
            name: self.name,
            description: self.description,
            display_name: self.display_name,
            typ: self.typ,
            col_id,
            status: self.status,
            scope: self.scope,
            nullable: self.nullable,
            is_array: self.is_array,
            is_dictionary: self.is_dictionary,
            dictionary_type: self.dictionary_type,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct UpdateEventPropertyRequest {
    pub id: u64,
    pub created_by: u64,
    pub updated_by: Option<u64>,
    pub project_id: u64,
    pub scope: Scope,
    pub tags: Vec<String>,
    pub name: String,
    pub description: String,
    pub display_name: Option<String>,
    pub typ: DataType,
    pub status: Status,
    pub nullable: bool,
    // this also defines whether property is required or not
    pub is_array: bool,
    pub is_dictionary: bool,
    pub dictionary_type: Option<DataType>,
}

impl IndexValues for UpdateEventPropertyRequest {
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

impl UpdateEventPropertyRequest {
    pub fn into_event_property(self, prev: EventProperty, updated_at: DateTime<Utc>, updated_by: Option<u64>) -> EventProperty {
        EventProperty {
            id: self.id,
            created_at: prev.created_at,
            updated_at: Some(updated_at),
            created_by: self.created_by,
            updated_by,
            project_id: self.project_id,
            tags: self.tags,
            name: self.name,
            description: self.description,
            display_name: self.display_name,
            typ: self.typ,
            col_id: prev.col_id,
            status: self.status,
            scope: self.scope,
            nullable: self.nullable,
            is_array: self.is_array,
            is_dictionary: self.is_dictionary,
            dictionary_type: self.dictionary_type,
        }
    }
}