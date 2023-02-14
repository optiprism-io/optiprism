pub mod provider_impl;

use async_trait::async_trait;
use chrono::DateTime;
use chrono::Utc;
use common::query::event_segmentation::EventSegmentation;
use common::types::OptionalProperty;
pub use provider_impl::ProviderImpl;
use serde::Deserialize;
use serde::Serialize;

use crate::metadata::ListResponse;
use crate::Result;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum Type {
    EventSegmentation,
    Funnel,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum Query {
    EventSegmentation(EventSegmentation),
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Report {
    pub id: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
    pub created_by: u64,
    pub updated_by: Option<u64>,
    pub project_id: u64,
    pub tags: Option<Vec<String>>,
    pub name: String,
    pub description: Option<String>,
    #[serde(rename = "type")]
    pub typ: Type,
    pub query: Query,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CreateReportRequest {
    pub created_by: u64,
    pub tags: Option<Vec<String>>,
    pub name: String,
    pub description: Option<String>,
    #[serde(rename = "type")]
    pub typ: Type,
    pub query: Query,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct UpdateReportRequest {
    pub updated_by: u64,
    pub tags: OptionalProperty<Option<Vec<String>>>,
    pub name: OptionalProperty<String>,
    pub description: OptionalProperty<Option<String>>,
    pub typ: OptionalProperty<Type>,
    pub query: OptionalProperty<Query>,
}

#[async_trait]
pub trait Provider: Sync + Send {
    async fn create(
        &self,
        organization_id: u64,
        project_id: u64,
        req: CreateReportRequest,
    ) -> Result<Report>;
    async fn get_by_id(&self, organization_id: u64, project_id: u64, id: u64) -> Result<Report>;
    async fn list(&self, organization_id: u64, project_id: u64) -> Result<ListResponse<Report>>;
    async fn update(
        &self,
        organization_id: u64,
        project_id: u64,
        report_id: u64,
        req: UpdateReportRequest,
    ) -> Result<Report>;
    async fn delete(&self, organization_id: u64, project_id: u64, id: u64) -> Result<Report>;
}
