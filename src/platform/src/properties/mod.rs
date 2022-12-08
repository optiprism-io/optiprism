mod provider_impl;

use axum::async_trait;
use chrono::DateTime;
use chrono::Utc;
use common::types::OptionalProperty;
pub use provider_impl::ProviderImpl;
use serde::Deserialize;
use serde::Serialize;

use crate::Context;
use crate::datatype::{DataType, DictionaryDataType};
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
    pub data_type: DataType,
    pub status: Status,
    pub is_system: bool,
    pub nullable: bool,
    // this also defines whether property is required or not
    pub is_array: bool,
    pub is_dictionary: bool,
    pub dictionary_type: Option<DictionaryDataType>,
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
            typ: self.data_type.try_into()?,
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
            data_type: self.typ.try_into()?,
            status: self.status.into(),
            is_system: self.is_system,
            nullable: self.nullable,
            is_array: self.is_array,
            is_dictionary: self.is_dictionary,
            dictionary_type: self.dictionary_type.map(|v| v.try_into()).transpose()?,
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
