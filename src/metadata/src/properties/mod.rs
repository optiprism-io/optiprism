pub mod provider_impl;

use arrow::datatypes;
use async_trait::async_trait;
use chrono::DateTime;
use chrono::Utc;
use common::types::{DType, OptionalProperty};
use common::types::TIME_UNIT;
use common::DECIMAL_PRECISION;
use common::DECIMAL_SCALE;
use convert_case::Case;
use convert_case::Casing;
pub use provider_impl::ProviderImpl;
use serde::Deserialize;
use serde::Serialize;

use crate::metadata::ListResponse;
use crate::Result;

pub trait Provider: Sync + Send {
    fn create(
        &self,
        organization_id: u64,
        project_id: u64,
        req: CreatePropertyRequest,
    ) -> Result<Property>;
    fn get_or_create(
        &self,
        organization_id: u64,
        project_id: u64,
        req: CreatePropertyRequest,
    ) -> Result<Property>;
    fn get_by_id(&self, organization_id: u64, project_id: u64, id: u64) -> Result<Property>;
    fn get_by_name(&self, organization_id: u64, project_id: u64, name: &str) -> Result<Property>;
    fn list(&self, organization_id: u64, project_id: u64) -> Result<ListResponse<Property>>;
    fn update(
        &self,
        organization_id: u64,
        project_id: u64,
        property_id: u64,
        req: UpdatePropertyRequest,
    ) -> Result<Property>;
    fn delete(&self, organization_id: u64, project_id: u64, id: u64) -> Result<Property>;
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub enum Status {
    #[default]
    Enabled,
    Disabled,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum Type {
    Event,
    User,
}

impl Type {
    pub fn as_name(&self) -> &str {
        match self {
            Type::Event => "event_properties",
            Type::User => "user_properties",
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum DictionaryType {
    Int8,
    Int16,
    Int32,
    Int64,
}

impl From<DictionaryType> for datatypes::DataType {
    fn from(value: DictionaryType) -> Self {
        match value {
            DictionaryType::Int8 => datatypes::DataType::Int8,
            DictionaryType::Int16 => datatypes::DataType::Int16,
            DictionaryType::Int32 => datatypes::DataType::Int32,
            DictionaryType::Int64 => datatypes::DataType::Int64,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
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
    pub typ: Type,
    pub data_type: DType,
    pub status: Status,
    pub is_system: bool,
    pub nullable: bool,
    // this also defines whether property is required or not
    pub is_array: bool,
    pub is_dictionary: bool,
    pub dictionary_type: Option<DictionaryType>,
}

impl Property {
    pub fn column_name(&self) -> String {
        let mut name: String = self
            .name
            .chars()
            .filter(|c| c.is_ascii_alphabetic() || c.is_numeric() || c.is_whitespace() || c == &'_')
            .collect();
        name = name.to_case(Case::Snake);
        name = name.trim().to_string();
        let prefix = match self.typ {
            Type::Event => "event".to_string(),
            Type::User => "user".to_string(),
        };

        format!("{prefix}_{name}")
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct CreatePropertyRequest {
    pub created_by: u64,
    pub tags: Option<Vec<String>>,
    pub name: String,
    pub description: Option<String>,
    pub display_name: Option<String>,
    pub typ: Type,
    pub data_type: DType,
    pub status: Status,
    pub is_system: bool,
    pub nullable: bool,
    pub is_array: bool,
    pub is_dictionary: bool,
    pub dictionary_type: Option<DictionaryType>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct UpdatePropertyRequest {
    pub updated_by: u64,
    pub tags: OptionalProperty<Option<Vec<String>>>,
    pub name: OptionalProperty<String>,
    pub description: OptionalProperty<Option<String>>,
    pub display_name: OptionalProperty<Option<String>>,
    pub typ: OptionalProperty<Type>,
    pub data_type: OptionalProperty<DType>,
    pub status: OptionalProperty<Status>,
    pub is_system: OptionalProperty<bool>,
    pub nullable: OptionalProperty<bool>,
    pub is_array: OptionalProperty<bool>,
    pub is_dictionary: OptionalProperty<bool>,
    pub dictionary_type: OptionalProperty<Option<DictionaryType>>,
}
