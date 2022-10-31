pub mod provider_impl;
pub mod types;

use async_trait::async_trait;
use chrono::DateTime;
use chrono::Utc;
use common::types::OptionalProperty;
pub use provider_impl::ProviderImpl;
use serde::Deserialize;
use serde::Serialize;
pub use types::CreateOrganizationRequest;
pub use types::Organization;

use crate::metadata::ListResponse;
use crate::Result;
#[async_trait]
pub trait Provider: Sync + Send {
    async fn create(&self, req: CreateOrganizationRequest) -> Result<Organization>;
    async fn get_by_id(&self, id: u64) -> Result<Organization>;
    async fn list(&self) -> Result<ListResponse<Organization>>;
    async fn update(&self, org_id: u64, req: UpdateOrganizationRequest) -> Result<Organization>;
    async fn delete(&self, id: u64) -> Result<Organization>;
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Organization {
    pub id: u64,
    pub created_at: DateTime<Utc>,
    pub created_by: u64,
    pub updated_at: Option<DateTime<Utc>>,
    pub updated_by: Option<u64>,
    pub name: String,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct CreateOrganizationRequest {
    pub created_by: u64,
    pub name: String,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct UpdateOrganizationRequest {
    pub updated_by: u64,
    pub name: OptionalProperty<String>,
}
