use arrow::datatypes::DataType;
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