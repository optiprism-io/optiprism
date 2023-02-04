pub mod provider_impl;

use axum::async_trait;
use chrono::DateTime;
use chrono::Utc;
use common::types::OptionalProperty;
pub use provider_impl::ProviderImpl;
use serde::Deserialize;
use serde::Serialize;

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
        request: CreateDashboardRequest,
    ) -> Result<Dashboard>;
    async fn get_by_id(
        &self,
        ctx: Context,
        organization_id: u64,
        project_id: u64,
        id: u64,
    ) -> Result<Dashboard>;
    async fn list(
        &self,
        ctx: Context,
        organization_id: u64,
        project_id: u64,
    ) -> Result<ListResponse<Dashboard>>;
    async fn update(
        &self,
        ctx: Context,
        organization_id: u64,
        project_id: u64,
        event_id: u64,
        req: UpdateDashboardRequest,
    ) -> Result<Dashboard>;
    async fn delete(
        &self,
        ctx: Context,
        organization_id: u64,
        project_id: u64,
        id: u64,
    ) -> Result<Dashboard>;
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum Type {
    Report,
}

impl From<Type> for metadata::dashboards::Type {
    fn from(v: Type) -> Self {
        match v {
            Type::Report => metadata::dashboards::Type::Report,
        }
    }
}

impl From<metadata::dashboards::Type> for Type {
    fn from(v: metadata::dashboards::Type) -> Self {
        match v {
            metadata::dashboards::Type::Report => Type::Report,
        }
    }
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

impl From<Panel> for metadata::dashboards::Panel {
    fn from(value: Panel) -> Self {
        metadata::dashboards::Panel {
            typ: value.typ.into(),
            report_id: value.report_id,
            x: value.x,
            y: value.y,
            w: value.w,
            h: value.h,
        }
    }
}

impl From<metadata::dashboards::Panel> for Panel {
    fn from(value: metadata::dashboards::Panel) -> Self {
        Panel {
            typ: value.typ.into(),
            report_id: value.report_id,
            x: value.x,
            y: value.y,
            w: value.w,
            h: value.h,
        }
    }
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

impl From<metadata::dashboards::Dashboard> for Dashboard {
    fn from(value: metadata::dashboards::Dashboard) -> Self {
        Dashboard {
            id: value.id,
            created_at: value.created_at,
            updated_at: value.updated_at,
            created_by: value.created_by,
            updated_by: value.updated_by,
            project_id: value.project_id,
            tags: value.tags,
            name: value.name,
            description: value.description,
            panels: value.panels.into_iter().map(|v| v.into()).collect(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CreateDashboardRequest {
    pub tags: Option<Vec<String>>,
    pub name: String,
    pub description: Option<String>,
    pub panels: Vec<Panel>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateDashboardRequest {
    #[serde(default, skip_serializing_if = "OptionalProperty::is_none")]
    pub tags: OptionalProperty<Option<Vec<String>>>,
    #[serde(default, skip_serializing_if = "OptionalProperty::is_none")]
    pub name: OptionalProperty<String>,
    #[serde(default, skip_serializing_if = "OptionalProperty::is_none")]
    pub description: OptionalProperty<Option<String>>,
    #[serde(default, skip_serializing_if = "OptionalProperty::is_none")]
    pub panels: OptionalProperty<Vec<Panel>>,
}
