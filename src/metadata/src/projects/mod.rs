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
    fn create(&self, organization_id: u64, req: CreateProjectRequest) -> Result<Project>;
    fn get_by_id(&self, organization_id: u64, project_id: u64) -> Result<Project>;
    fn get_by_token(&self, token: &str) -> Result<Project>;
    fn list(&self, organization_id: u64) -> Result<ListResponse<Project>>;
    fn update(
        &self,
        organization_id: u64,
        project_id: u64,
        req: UpdateProjectRequest,
    ) -> Result<Project>;
    fn delete(&self, organization_id: u64, project_id: u64) -> Result<Project>;
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Project {
    pub id: u64,
    pub created_at: DateTime<Utc>,
    pub created_by: u64,
    pub updated_at: Option<DateTime<Utc>>,
    pub updated_by: Option<u64>,
    pub organization_id: u64,
    pub name: String,
    pub token: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CreateProjectRequest {
    pub created_by: u64,
    pub name: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct UpdateProjectRequest {
    pub updated_by: u64,
    pub name: OptionalProperty<String>,
}
