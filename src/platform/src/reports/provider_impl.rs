use std::sync::Arc;

use axum::async_trait;
use common::rbac::ProjectPermission;
use metadata::reports::Provider as ReportsProvider;

use crate::reports::CreateReportRequest;
use crate::reports::Provider;
use crate::reports::Report;
use crate::reports::UpdateReportRequest;
use crate::Context;
use crate::ListResponse;
use crate::Result;

pub struct ProviderImpl {
    prov: Arc<dyn ReportsProvider>,
}

impl ProviderImpl {
    pub fn new(prov: Arc<dyn ReportsProvider>) -> Self {
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
        request: CreateReportRequest,
    ) -> Result<Report> {
        ctx.check_project_permission(
            organization_id,
            project_id,
            ProjectPermission::ManageReports,
        )?;

        let report = self
            .prov
            .create(
                organization_id,
                project_id,
                metadata::reports::CreateReportRequest {
                    created_by: ctx.account_id.unwrap(),
                    tags: request.tags,
                    name: request.name,
                    description: request.description,
                    typ: request.typ.into(),
                    query: request.query.into(),
                },
            )
            .await?;

        Ok(report.into())
    }

    async fn get_by_id(
        &self,
        ctx: Context,
        organization_id: u64,
        project_id: u64,
        id: u64,
    ) -> Result<Report> {
        ctx.check_project_permission(
            organization_id,
            project_id,
            ProjectPermission::ExploreReports,
        )?;

        Ok(self
            .prov
            .get_by_id(organization_id, project_id, id)
            .await?
            .into())
    }

    async fn list(
        &self,
        ctx: Context,
        organization_id: u64,
        project_id: u64,
    ) -> Result<ListResponse<Report>> {
        ctx.check_project_permission(
            organization_id,
            project_id,
            ProjectPermission::ExploreReports,
        )?;
        let resp = self.prov.list(organization_id, project_id).await?;

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
        report_id: u64,
        req: UpdateReportRequest,
    ) -> Result<Report> {
        ctx.check_project_permission(
            organization_id,
            project_id,
            ProjectPermission::ManageReports,
        )?;

        let md_req = metadata::reports::UpdateReportRequest {
            updated_by: ctx.account_id.unwrap(),
            tags: req.tags,
            name: req.name,
            description: req.description,
            typ: req.typ.into(),
            query: req.query.into(),
        };

        let report = self
            .prov
            .update(organization_id, project_id, report_id, md_req)
            .await?;

        Ok(report.into())
    }

    async fn delete(
        &self,
        ctx: Context,
        organization_id: u64,
        project_id: u64,
        id: u64,
    ) -> Result<Report> {
        ctx.check_project_permission(
            organization_id,
            project_id,
            ProjectPermission::ManageReports,
        )?;

        Ok(self
            .prov
            .delete(organization_id, project_id, id)
            .await?
            .into())
    }
}
