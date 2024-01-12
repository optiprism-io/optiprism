pub mod provider_impl;

use chrono::DateTime;
use chrono::Utc;
use common::types::OptionalProperty;
pub use provider_impl::ProviderImpl;
use serde::Deserialize;
use serde::Serialize;

use crate::metadata::ListResponse;
use crate::Result;

pub trait Provider: Sync + Send {
    fn create(&self, req: CreateOrganizationRequest) -> Result<Organization>;
    fn get_by_id(&self, id: u64) -> Result<Organization>;
    fn list(&self) -> Result<ListResponse<Organization>>;
    fn update(&self, org_id: u64, req: UpdateOrganizationRequest) -> Result<Organization>;
    fn delete(&self, id: u64) -> Result<Organization>;
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
