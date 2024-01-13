use std::sync::Arc;

use axum::async_trait;
use common::rbac::ProjectPermission;
use metadata::dashboards::Dashboards;

use crate::dashboards::CreateDashboardRequest;
use crate::dashboards::Dashboard;
use crate::dashboards::Provider;
use crate::dashboards::UpdateDashboardRequest;
use crate::Context;
use crate::ListResponse;
use crate::Result;

pub struct ProviderImpl {
    prov: Arc<Dashboards>,
}

impl ProviderImpl {
    pub fn new(prov: Arc<Dashboards>) -> Self {
        Self { prov }
    }
}

#[async_trait]
impl Provider for ProviderImpl {
    async fn create(
        &self,
        ctx: Context,
        organization_id: u64,
        project_id: u64,
        request: CreateDashboardRequest,
    ) -> Result<Dashboard> {
        ctx.check_project_permission(
            organization_id,
            project_id,
            ProjectPermission::ManageReports,
        )?;

        let dashboard = self.prov.create(
            organization_id,
            project_id,
            metadata::dashboards::CreateDashboardRequest {
                created_by: ctx.account_id.unwrap(),
                tags: request.tags,
                name: request.name,
                description: request.description,
                panels: request.panels.into_iter().map(|v| v.into()).collect(),
            },
        )?;

        Ok(dashboard.into())
    }

    async fn get_by_id(
        &self,
        ctx: Context,
        organization_id: u64,
        project_id: u64,
        id: u64,
    ) -> Result<Dashboard> {
        ctx.check_project_permission(
            organization_id,
            project_id,
            ProjectPermission::ExploreReports,
        )?;

        Ok(self.prov.get_by_id(organization_id, project_id, id)?.into())
    }

    async fn list(
        &self,
        ctx: Context,
        organization_id: u64,
        project_id: u64,
    ) -> Result<ListResponse<Dashboard>> {
        ctx.check_project_permission(
            organization_id,
            project_id,
            ProjectPermission::ExploreReports,
        )?;
        let resp = self.prov.list(organization_id, project_id)?;

        Ok(ListResponse {
            data: resp.data.into_iter().map(|v| v.into()).collect(),
            meta: resp.meta.into(),
        })
    }

    async fn update(
        &self,
        ctx: Context,
        organization_id: u64,
        project_id: u64,
        dashboard_id: u64,
        req: UpdateDashboardRequest,
    ) -> Result<Dashboard> {
        ctx.check_project_permission(
            organization_id,
            project_id,
            ProjectPermission::ManageReports,
        )?;

        let md_req = metadata::dashboards::UpdateDashboardRequest {
            updated_by: ctx.account_id.unwrap(),
            tags: req.tags,
            name: req.name,
            description: req.description,
            panels: req
                .panels
                .map(|v| v.into_iter().map(|v| v.into()).collect()),
        };

        let dashboard = self
            .prov
            .update(organization_id, project_id, dashboard_id, md_req)?;

        Ok(dashboard.into())
    }

    async fn delete(
        &self,
        ctx: Context,
        organization_id: u64,
        project_id: u64,
        id: u64,
    ) -> Result<Dashboard> {
        ctx.check_project_permission(
            organization_id,
            project_id,
            ProjectPermission::ManageReports,
        )?;

        Ok(self.prov.delete(organization_id, project_id, id)?.into())
    }
}
