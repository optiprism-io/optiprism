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
    fn from(value: metadata::reports::Type) -> Self {
        match value {
            metadata::reports::Type::EventSegmentation => Type::EventSegmentation,
            metadata::reports::Type::Funnel => Type::Funnel,
        }
    }
}

impl From<Type> for metadata::reports::Type {
    fn from(value: Type) -> Self {
        match value {
            Type::EventSegmentation => metadata::reports::Type::EventSegmentation,
            Type::Funnel => metadata::reports::Type::Funnel,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum Query {
    EventSegmentation(EventSegmentation),
}

impl From<metadata::reports::Query> for Query {
    fn from(value: metadata::reports::Query) -> Self {
        match value {
            metadata::reports::Query::EventSegmentation(es) => {
                Query::EventSegmentation(es.try_into().unwrap())
            }
        }
    }
}

impl From<Query> for metadata::reports::Query {
    fn from(value: Query) -> Self {
        match value {
            Query::EventSegmentation(es) => {
                metadata::reports::Query::EventSegmentation(es.try_into().unwrap())
            }
        }
    }
}

impl From<metadata::reports::Report> for Report {
    fn from(value: metadata::reports::Report) -> Self {
        Report {
            id: value.id,
            created_at: value.created_at,
            updated_at: value.updated_at,
            created_by: value.created_by,
            updated_by: value.updated_by,
            project_id: value.project_id,
            tags: value.tags,
            name: value.name,
            description: value.description,
            typ: value.typ.into(),
            query: value.query.into(),
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
    pub name: OptionalProperty<String>,
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
