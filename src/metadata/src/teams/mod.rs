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
    async fn create(&self, organization_id: u64, req: CreateTeamRequest) -> Result<Team>;
    async fn get_by_id(&self, organization_id: u64, team_id: u64) -> Result<Team>;
    async fn list(&self, organization_id: u64) -> Result<ListResponse<Team>>;
    async fn update(
        &self,
        organization_id: u64,
        team_id: u64,
        req: UpdateTeamRequest,
    ) -> Result<Team>;
    async fn delete(&self, organization_id: u64, team_id: u64) -> Result<Team>;
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Team {
    pub id: u64,
    pub created_at: DateTime<Utc>,
    pub created_by: u64,
    pub updated_at: Option<DateTime<Utc>>,
    pub updated_by: Option<u64>,
    pub organization_id: u64,
    pub name: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CreateTeamRequest {
    pub created_by: u64,
    pub organization_id: u64,
    pub name: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct UpdateTeamRequest {
    pub updated_by: u64,
    pub name: OptionalProperty<String>,
}
