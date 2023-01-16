pub mod provider_impl;

use axum::async_trait;
use chrono::DateTime;
use chrono::Utc;
use common::types::OptionalProperty;
pub use provider_impl::ProviderImpl;
use serde::Deserialize;
use serde::Serialize;

use crate::queries::event_segmentation::EventSegmentation;
use crate::Context;
use crate::ListResponse;
use crate::PlatformError;
use crate::Result;

#[async_trait]
pub trait Provider: Sync + Send {
    async fn create(
        &self,
        ctx: Context,
        organization_id: u64,
        project_id: u64,
        request: CreateReportRequest,
    ) -> Result<Report>;
    async fn get_by_id(
        &self,
        ctx: Context,
        organization_id: u64,
        project_id: u64,
        id: u64,
    ) -> Result<Report>;
    async fn list(
        &self,
        ctx: Context,
        organization_id: u64,
        project_id: u64,
    ) -> Result<ListResponse<Report>>;
    async fn update(
        &self,
        ctx: Context,
        organization_id: u64,
        project_id: u64,
        event_id: u64,
        req: UpdateReportRequest,
    ) -> Result<Report>;
    async fn delete(
        &self,
        ctx: Context,
        organization_id: u64,
        project_id: u64,
        id: u64,
    ) -> Result<Report>;
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum Type {
    EventSegmentation,
    Funnel,
}

impl From<metadata::reports::Type> for Type {
    fn from(t: metadata::reports::Type) -> Self {
        match t {
            metadata::reports::Type::EventSegmentation => Type::EventSegmentation,
            metadata::reports::Type::Funnel => Type::Funnel,
        }
    }
}

impl From<Type> for metadata::reports::Type {
    fn from(t: Type) -> Self {
        match t {
            Type::EventSegmentation => metadata::reports::Type::EventSegmentation,
            Type::Funnel => metadata::reports::Type::Funnel,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(untagged)]
pub enum Query {
    EventSegmentation(EventSegmentation),
}

impl From<metadata::reports::Query> for Query {
    fn from(q: metadata::reports::Query) -> Self {
        match q {
            metadata::reports::Query::EventSegmentation(event_seg) => {
                Query::EventSegmentation(event_seg.clone())
            }
        }
    }
}

impl From<Query> for metadata::reports::Query {
    fn from(q: Query) -> Self {
        match q {
            Query::EventSegmentation(event_seg) => {
                metadata::reports::Query::EventSegmentation(event_seg.clone())
            }
        }
    }
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

impl TryInto<metadata::reports::Report> for Report {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<metadata::reports::Report, Self::Error> {
        Ok(metadata::reports::Report {
            id: self.id,
            created_at: self.created_at,
            updated_at: self.updated_at,
            created_by: self.created_by,
            updated_by: self.updated_by,
            project_id: self.project_id,
            tags: self.tags,
            name: self.name,
            description: self.description,
            typ: self.typ.into(),
            query: self.query.into(),
        })
    }
}

impl TryInto<Report> for metadata::reports::Report {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<Report, Self::Error> {
        Ok(Report {
            id: self.id,
            created_at: self.created_at,
            updated_at: self.updated_at,
            created_by: self.created_by,
            updated_by: self.updated_by,
            project_id: self.project_id,
            tags: self.tags,
            name: self.name,
            description: self.description,
            typ: self.typ.into(),
            query: self.query.into(),
        })
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CreateReportRequest {
    pub tags: Option<Vec<String>>,
    pub name: String,
    pub description: Option<String>,
    #[serde(rename = "type")]
    pub typ: Type,
    pub query: Query,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateReportRequest {
    #[serde(default, skip_serializing_if = "OptionalProperty::is_none")]
    pub tags: OptionalProperty<Option<Vec<String>>>,
    #[serde(default, skip_serializing_if = "OptionalProperty::is_none")]
    pub name: OptionalProperty<Option<String>>,
    #[serde(default, skip_serializing_if = "OptionalProperty::is_none")]
    pub description: OptionalProperty<Option<String>>,
    #[serde(
        default,
        rename = "type",
        skip_serializing_if = "OptionalProperty::is_none"
    )]
    pub typ: OptionalProperty<Type>,
    #[serde(default, skip_serializing_if = "OptionalProperty::is_none")]
    pub query: OptionalProperty<Query>,
}
