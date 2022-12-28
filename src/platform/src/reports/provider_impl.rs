use std::sync::Arc;

use axum::async_trait;
use common::rbac::ProjectPermission;

use super::CreateReportRequest;
use super::Provider;
use super::Report;
use crate::Context;

pub struct ProviderImpl {
    // prov: Arc<dyn reports::Provider>, // <- metadata::reports::Provider to be used here
    prov: Arc<dyn crate::reports::Provider>,
}

impl ProviderImpl {
    // pub fn new(prov: Arc<dyn reports::Provider>) -> Self { // <- metadata::reports::Provider to be used here
    pub fn new(prov: Arc<dyn crate::reports::Provider>) -> Self {
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
        ctx.check_project_permission(organization_id, project_id, ProjectPermission::ManageSchema)?;

        let report = self
            .prov
            .create(
                ctx, // FIXME: remove
                organization_id,
                project_id,
                // metadata::reports::CreateReportRequest {
                CreateReportRequest {
                    // created_by: ctx.account_id.unwrap(),
                    tags: request.tags,
                    name: request.name,
                    description: request.description,
                    typ: request.typ, // ? typ or type
                    query: request.query,
                },
            )
            .await?;

        report.try_into()
    }
}
