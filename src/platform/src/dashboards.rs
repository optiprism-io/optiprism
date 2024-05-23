use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use common::rbac::ProjectPermission;
use common::types::OptionalProperty;
use metadata::dashboards::Dashboards as MDDashboards;
use serde::Deserialize;
use serde::Serialize;

use crate::Context;
use crate::ListResponse;
use crate::Result;

pub struct Dashboards {
    prov: Arc<MDDashboards>,
}

impl Dashboards {
    pub fn new(prov: Arc<MDDashboards>) -> Self {
        Self { prov }
    }
    pub async fn create(
        &self,
        ctx: Context,

        project_id: u64,
        request: CreateDashboardRequest,
    ) -> Result<Dashboard> {
        ctx.check_project_permission(
            ctx.organization_id,
            project_id,
            ProjectPermission::ManageReports,
        )?;

        let dashboard =
            self.prov
                .create(project_id, metadata::dashboards::CreateDashboardRequest {
                    created_by: ctx.account_id,
                    tags: request.tags,
                    name: request.name,
                    description: request.description,
                    panels: request.panels.into_iter().map(|v| v.into()).collect(),
                })?;

        Ok(dashboard.into())
    }

    pub async fn get_by_id(&self, ctx: Context, project_id: u64, id: u64) -> Result<Dashboard> {
        ctx.check_project_permission(
            ctx.organization_id,
            project_id,
            ProjectPermission::ExploreReports,
        )?;

        Ok(self.prov.get_by_id(project_id, id)?.into())
    }

    pub async fn list(&self, ctx: Context, project_id: u64) -> Result<ListResponse<Dashboard>> {
        ctx.check_project_permission(
            ctx.organization_id,
            project_id,
            ProjectPermission::ExploreReports,
        )?;
        let resp = self.prov.list(project_id)?;

        Ok(ListResponse {
            data: resp.data.into_iter().map(|v| v.into()).collect(),
            meta: resp.meta.into(),
        })
    }

    pub async fn update(
        &self,
        ctx: Context,

        project_id: u64,
        dashboard_id: u64,
        req: UpdateDashboardRequest,
    ) -> Result<Dashboard> {
        ctx.check_project_permission(
            ctx.organization_id,
            project_id,
            ProjectPermission::ManageReports,
        )?;

        let md_req = metadata::dashboards::UpdateDashboardRequest {
            updated_by: ctx.account_id,
            tags: req.tags,
            name: req.name,
            description: req.description,
            panels: req
                .panels
                .map(|v| v.into_iter().map(|v| v.into()).collect()),
        };

        let dashboard = self.prov.update(project_id, dashboard_id, md_req)?;

        Ok(dashboard.into())
    }

    pub async fn delete(&self, ctx: Context, project_id: u64, id: u64) -> Result<Dashboard> {
        ctx.check_project_permission(
            ctx.organization_id,
            project_id,
            ProjectPermission::ManageReports,
        )?;

        Ok(self.prov.delete(project_id, id)?.into())
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum Type {
    Report,
}

impl From<crate::dashboards::Type> for metadata::dashboards::Type {
    fn from(v: crate::dashboards::Type) -> Self {
        match v {
            crate::dashboards::Type::Report => metadata::dashboards::Type::Report,
        }
    }
}

impl From<metadata::dashboards::Type> for crate::dashboards::Type {
    fn from(v: metadata::dashboards::Type) -> Self {
        match v {
            metadata::dashboards::Type::Report => crate::dashboards::Type::Report,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Panel {
    #[serde(rename = "type")]
    pub typ: crate::dashboards::Type,
    pub report_id: u64,
    pub x: usize,
    pub y: usize,
    pub w: usize,
    pub h: usize,
}

impl From<crate::dashboards::Panel> for metadata::dashboards::Panel {
    fn from(value: crate::dashboards::Panel) -> Self {
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

impl From<metadata::dashboards::Panel> for crate::dashboards::Panel {
    fn from(value: metadata::dashboards::Panel) -> Self {
        crate::dashboards::Panel {
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
    pub panels: Vec<crate::dashboards::Panel>,
}

impl From<metadata::dashboards::Dashboard> for crate::dashboards::Dashboard {
    fn from(value: metadata::dashboards::Dashboard) -> Self {
        crate::dashboards::Dashboard {
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
    pub panels: Vec<crate::dashboards::Panel>,
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
    pub panels: OptionalProperty<Vec<crate::dashboards::Panel>>,
}
