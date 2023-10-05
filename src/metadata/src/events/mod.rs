pub mod provider_impl;

use async_trait::async_trait;
use chrono::DateTime;
use chrono::Utc;
use common::types::OptionalProperty;
pub use provider_impl::ProviderImpl;
use serde::Deserialize;
use serde::Serialize;

use crate::metadata::ListResponse;
use crate::Result;

#[async_trait]
pub trait Provider: Sync + Send {
    async fn create(
        &self,
        organization_id: u64,
        project_id: u64,
        req: CreateEventRequest,
    ) -> Result<Event>;

    async fn get_or_create(
        &self,
        organization_id: u64,
        project_id: u64,
        req: CreateEventRequest,
    ) -> Result<Event>;

    async fn get_by_id(&self, organization_id: u64, project_id: u64, id: u64) -> Result<Event>;

    async fn get_by_name(&self, organization_id: u64, project_id: u64, name: &str)
    -> Result<Event>;

    async fn list(&self, organization_id: u64, project_id: u64) -> Result<ListResponse<Event>>;

    async fn update(
        &self,
        organization_id: u64,
        project_id: u64,
        event_id: u64,
        req: UpdateEventRequest,
    ) -> Result<Event>;

    async fn attach_property(
        &self,
        organization_id: u64,
        project_id: u64,
        event_id: u64,
        prop_id: u64,
    ) -> Result<Event>;

    async fn detach_property(
        &self,
        organization_id: u64,
        project_id: u64,
        event_id: u64,
        prop_id: u64,
    ) -> Result<Event>;
    async fn delete(&self, organization_id: u64, project_id: u64, id: u64) -> Result<Event>;
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub enum Status {
    #[default]
    Enabled,
    Disabled,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
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

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct CreateEventRequest {
    pub created_by: u64,
    pub tags: Option<Vec<String>>,
    pub name: String,
    pub display_name: Option<String>,
    pub description: Option<String>,
    pub status: Status,
    pub is_system: bool,
    pub properties: Option<Vec<u64>>,
    pub custom_properties: Option<Vec<u64>>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct UpdateEventRequest {
    pub updated_by: u64,
    pub tags: OptionalProperty<Option<Vec<String>>>,
    pub name: OptionalProperty<String>,
    pub display_name: OptionalProperty<Option<String>>,
    pub description: OptionalProperty<Option<String>>,
    pub status: OptionalProperty<Status>,
    pub is_system: OptionalProperty<bool>,
    pub properties: OptionalProperty<Option<Vec<u64>>>,
    pub custom_properties: OptionalProperty<Option<Vec<u64>>>,
}
