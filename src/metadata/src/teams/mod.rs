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
    fn create(&self, organization_id: u64, req: CreateTeamRequest) -> Result<Team>;
    fn get_by_id(&self, organization_id: u64, team_id: u64) -> Result<Team>;
    fn list(&self, organization_id: u64) -> Result<ListResponse<Team>>;
    fn update(&self, organization_id: u64, team_id: u64, req: UpdateTeamRequest) -> Result<Team>;
    fn delete(&self, organization_id: u64, team_id: u64) -> Result<Team>;
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
