mod provider_impl;

use axum::async_trait;
use chrono::DateTime;
use chrono::Utc;
use common::types::OptionalProperty;
pub use provider_impl::ProviderImpl;
use serde::Deserialize;
use serde::Serialize;

use crate::datatype::DictionaryType;
use crate::Context;
use crate::ListResponse;
use crate::PlatformError;
use crate::Result;

#[async_trait]
pub trait Provider: Sync + Send {
    async fn get_by_id(
        &self,
        ctx: Context,
        organization_id: u64,
        project_id: u64,
        id: u64,
    ) -> Result<Property>;
    async fn get_by_name(
        &self,
        ctx: Context,
        organization_id: u64,
        project_id: u64,
        name: &str,
    ) -> Result<Property>;
    async fn list(
        &self,
        ctx: Context,
        organization_id: u64,
        project_id: u64,
    ) -> Result<ListResponse<Property>>;
    async fn update(
        &self,
        ctx: Context,
        organization_id: u64,
        project_id: u64,
        property_id: u64,
        req: UpdatePropertyRequest,
    ) -> Result<Property>;
    async fn delete(
        &self,
        ctx: Context,
        organization_id: u64,
        project_id: u64,
        id: u64,
    ) -> Result<Property>;
}

#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum Status {
    #[default]
    Enabled,
    Disabled,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum Type {
    Event,
    User,
}

impl From<metadata::properties::Type> for Type {
    fn from(value: metadata::properties::Type) -> Self {
        match value {
            metadata::properties::Type::Event => Type::Event,
            metadata::properties::Type::User => Type::User,
        }
    }
}

impl From<Type> for metadata::properties::Type {
    fn from(value: Type) -> Self {
        match value {
            Type::Event => metadata::properties::Type::Event,
            Type::User => metadata::properties::Type::User,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum DataType {
    String,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float64,
    Decimal,
    Boolean,
    Timestamp,
}

impl From<metadata::properties::DataType> for DataType {
    fn from(value: metadata::properties::DataType) -> Self {
        match value {
            metadata::properties::DataType::String => DataType::String,
            metadata::properties::DataType::Int8 => DataType::Int8,
            metadata::properties::DataType::Int16 => DataType::Int16,
            metadata::properties::DataType::Int32 => DataType::Int32,
            metadata::properties::DataType::Int64 => DataType::Int64,
            metadata::properties::DataType::UInt8 => DataType::UInt8,
            metadata::properties::DataType::UInt16 => DataType::UInt16,
            metadata::properties::DataType::UInt32 => DataType::UInt32,
            metadata::properties::DataType::UInt64 => DataType::UInt64,
            metadata::properties::DataType::Float64 => DataType::Float64,
            metadata::properties::DataType::Decimal => DataType::Decimal,
            metadata::properties::DataType::Boolean => DataType::Boolean,
            metadata::properties::DataType::Timestamp => DataType::Timestamp,
        }
    }
}

impl From<DataType> for metadata::properties::DataType {
    fn from(value: DataType) -> Self {
        match value {
            DataType::String => metadata::properties::DataType::String,
            DataType::Int8 => metadata::properties::DataType::Int8,
            DataType::Int16 => metadata::properties::DataType::Int16,
            DataType::Int32 => metadata::properties::DataType::Int32,
            DataType::Int64 => metadata::properties::DataType::Int64,
            DataType::UInt8 => metadata::properties::DataType::UInt8,
            DataType::UInt16 => metadata::properties::DataType::UInt16,
            DataType::UInt32 => metadata::properties::DataType::UInt32,
            DataType::UInt64 => metadata::properties::DataType::UInt64,
            DataType::Float64 => metadata::properties::DataType::Float64,
            DataType::Decimal => metadata::properties::DataType::Decimal,
            DataType::Boolean => metadata::properties::DataType::Boolean,
            DataType::Timestamp => metadata::properties::DataType::Timestamp,
        }
    }
}

impl From<metadata::properties::Status> for Status {
    fn from(s: metadata::properties::Status) -> Self {
        match s {
            metadata::properties::Status::Enabled => Status::Enabled,
            metadata::properties::Status::Disabled => Status::Disabled,
        }
    }
}

impl From<Status> for metadata::properties::Status {
    fn from(s: Status) -> Self {
        match s {
            Status::Enabled => metadata::properties::Status::Enabled,
            Status::Disabled => metadata::properties::Status::Disabled,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Property {
    pub id: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
    pub created_by: u64,
    pub updated_by: Option<u64>,
    pub project_id: u64,
    pub events: Option<Vec<u64>>,
    pub tags: Option<Vec<String>>,
    pub name: String,
    pub display_name: Option<String>,
    pub description: Option<String>,
    pub typ: Type,
    pub data_type: DataType,
    pub status: Status,
    pub is_system: bool,
    pub nullable: bool,
    // this also defines whether property is required or not
    pub is_array: bool,
    pub is_dictionary: bool,
    pub dictionary_type: Option<DictionaryType>,
}

impl TryInto<metadata::properties::Property> for Property {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<metadata::properties::Property, Self::Error> {
        Ok(metadata::properties::Property {
            id: self.id,
            created_at: self.created_at,
            updated_at: self.updated_at,
            created_by: self.created_by,
            updated_by: self.updated_by,
            project_id: self.project_id,
            tags: self.tags,
            name: self.name,
            description: self.description,
            display_name: self.display_name,
            typ: self.typ.into(),
            data_type: self.data_type.into(),
            status: self.status.into(),
            is_system: self.is_system,
            nullable: self.nullable,
            is_array: self.is_array,
            is_dictionary: self.is_dictionary,
            dictionary_type: self.dictionary_type.map(|v| v.try_into()).transpose()?,
        })
    }
}

impl TryInto<Property> for metadata::properties::Property {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<Property, Self::Error> {
        Ok(Property {
            id: self.id,
            created_at: self.created_at,
            updated_at: self.updated_at,
            created_by: self.created_by,
            updated_by: self.updated_by,
            project_id: self.project_id,
            events: None,
            tags: self.tags,
            name: self.name,
            description: self.description,
            display_name: self.display_name,
            data_type: self.data_type.into(),
            status: self.status.into(),
            is_system: self.is_system,
            nullable: self.nullable,
            is_array: self.is_array,
            is_dictionary: self.is_dictionary,
            dictionary_type: self.dictionary_type.map(|v| v.try_into()).transpose()?,
            typ: self.typ.into(),
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdatePropertyRequest {
    #[serde(default, skip_serializing_if = "OptionalProperty::is_none")]
    pub tags: OptionalProperty<Option<Vec<String>>>,
    #[serde(default, skip_serializing_if = "OptionalProperty::is_none")]
    pub display_name: OptionalProperty<Option<String>>,
    #[serde(default, skip_serializing_if = "OptionalProperty::is_none")]
    pub description: OptionalProperty<Option<String>>,
    #[serde(default, skip_serializing_if = "OptionalProperty::is_none")]
    pub status: OptionalProperty<Status>,
}
