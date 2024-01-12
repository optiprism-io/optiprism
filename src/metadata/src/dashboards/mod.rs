pub mod provider_impl;

use chrono::DateTime;
use chrono::Utc;
use common::types::OptionalProperty;
pub use provider_impl::ProviderImpl;
use serde::Deserialize;
use serde::Serialize;

use crate::metadata::ListResponse;
use crate::Result;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum Type {
    Report,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Panel {
    #[serde(rename = "type")]
    pub typ: Type,
    pub report_id: u64,
    pub x: usize,
    pub y: usize,
    pub w: usize,
    pub h: usize,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Dashboard {
    pub id: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
    pub created_by: u64,
    pub updated_by: Option<u64>,
    pub project_id: u64,
    pub tags: Option<Vec<String>>,
    pub name: String,
    pub description: Option<String>,
    pub panels: Vec<Panel>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CreateDashboardRequest {
    pub created_by: u64,
    pub tags: Option<Vec<String>>,
    pub name: String,
    pub description: Option<String>,
    pub panels: Vec<Panel>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct UpdateDashboardRequest {
    pub updated_by: u64,
    pub tags: OptionalProperty<Option<Vec<String>>>,
    pub name: OptionalProperty<String>,
    pub description: OptionalProperty<Option<String>>,
    pub panels: OptionalProperty<Vec<Panel>>,
}

pub trait Provider: Sync + Send {
    fn create(
        &self,
        organization_id: u64,
        project_id: u64,
        req: CreateDashboardRequest,
    ) -> Result<Dashboard>;
    fn get_by_id(&self, organization_id: u64, project_id: u64, id: u64) -> Result<Dashboard>;
    fn list(&self, organization_id: u64, project_id: u64) -> Result<ListResponse<Dashboard>>;
    fn update(
        &self,
        organization_id: u64,
        project_id: u64,
        dashboard_id: u64,
        req: UpdateDashboardRequest,
    ) -> Result<Dashboard>;
    fn delete(&self, organization_id: u64, project_id: u64, id: u64) -> Result<Dashboard>;
}
