use async_trait::async_trait;
use chrono::DateTime;
use chrono::Utc;
use common::queries::event_segmentation::Analysis;
use common::queries::event_segmentation::Breakdown;
use common::queries::event_segmentation::ChartType;
use common::queries::event_segmentation::Compare;
use common::queries::event_segmentation::Event;
use common::queries::QueryTime;
use common::queries::TimeIntervalUnit;
use common::types::EventFilter;
use common::types::OptionalProperty;
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
        request: CreateReportRequest,
    ) -> Result<Report>;
    async fn get_by_id(&self, organization_id: u64, project_id: u64, id: u64) -> Result<Report>;
    async fn list(&self, organization_id: u64, project_id: u64) -> Result<ListResponse<Report>>;
    async fn update(
        &self,
        organization_id: u64,
        project_id: u64,
        event_id: u64,
        req: UpdateReportRequest,
    ) -> Result<Report>;
    async fn delete(&self, organization_id: u64, project_id: u64, id: u64) -> Result<Report>;
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
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
pub enum Type {
    EventSegmentation,
    Funnel,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct EventSegmentation {
    pub time: QueryTime,
    pub group: String,
    pub interval_unit: TimeIntervalUnit,
    pub chart_type: ChartType,
    pub analysis: Analysis,
    pub compare: Option<Compare>,
    pub events: Vec<Event>,
    pub filters: Option<Vec<EventFilter>>,
    pub breakdowns: Option<Vec<Breakdown>>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum Query {
    EventSegmentation(EventSegmentation),
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct CreateReportRequest {
    pub created_by: u64,
    pub tags: Option<Vec<String>>,
    pub name: String,
    pub description: Option<String>,
    #[serde(rename = "type")]
    pub typ: Type,
    pub query: Query,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct UpdateReportRequest {
    pub updated_by: u64,
    pub tags: OptionalProperty<Option<Vec<String>>>,
    pub name: OptionalProperty<String>,
    pub description: OptionalProperty<Option<String>>,
    #[serde(rename = "type")]
    pub typ: OptionalProperty<Type>,
    pub query: OptionalProperty<Query>,
}
