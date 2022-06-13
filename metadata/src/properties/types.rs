use crate::properties::provider::Namespace;
use arrow::datatypes::DataType;
use chrono::{DateTime, Utc};
use convert_case::{Case, Casing};
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
pub struct Property {
    pub id: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
    pub created_by: u64,
    pub updated_by: Option<u64>,
    pub project_id: u64,
    pub tags: Option<Vec<String>>,
    pub name: String,
    pub description: Option<String>,
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

impl Property {
    pub fn column_name(&self, ns: Namespace) -> String {
        let mut name: String = self
            .name
            .chars()
            .filter(|c| c.is_ascii_alphabetic() || c.is_numeric() || c.is_whitespace())
            .collect();
        name = name.to_case(Case::Snake);
        name = name.trim().to_string();
        let prefix = match ns {
            Namespace::Event => "event".to_string(),
            Namespace::User => "user".to_string(),
        };

        format!("{}_{}", prefix, name)
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct CreatePropertyRequest {
    pub created_by: u64,
    pub tags: Option<Vec<String>>,
    pub name: String,
    pub description: Option<String>,
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
pub struct UpdatePropertyRequest {
    pub created_by: u64,
    pub updated_by: Option<u64>,
    pub scope: Scope,
    pub tags: Option<Vec<String>>,
    pub name: String,
    pub description: Option<String>,
    pub display_name: Option<String>,
    pub typ: DataType,
    pub status: Status,
    pub nullable: bool,
    // this also defines whether property is required or not
    pub is_array: bool,
    pub is_dictionary: bool,
    pub dictionary_type: Option<DataType>,
}
