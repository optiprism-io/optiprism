use chrono::{DateTime, Utc};
use datafusion::arrow::datatypes::DataType;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum DBCol {
    Named(String),
    Order(usize),
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum Status {
    Enabled,
    Disabled,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum Scope {
    Event(u64),
    Global,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct EventProperty {
    pub id: u64,
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
    pub created_by: u64,
    pub updated_by: u64,
    pub project_id: u64,
    pub scope: Scope,
    pub tags: Vec<String>,
    pub name: String,
    pub description: String,
    pub display_name: String,
    pub typ: DataType,
    pub db_col: DBCol,
    pub status: Status,
    pub nullable: bool,
    // this also defines whether property is required or not
    pub is_array: bool,
    pub is_dictionary: bool,
    pub dictionary_type: Option<DataType>,
}