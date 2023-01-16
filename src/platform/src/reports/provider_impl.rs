use std::sync::Arc;

use axum::async_trait;
use common::rbac::ProjectPermission;
use metadata::reports::Provider as ReportsProvider;

use super::CreateReportRequest;
use super::Provider;
use super::Report;
use crate::Context;
use crate::PlatformError;

pub struct ProviderImpl {
    prov: Arc<dyn ReportsProvider>,
}

impl ProviderImpl {
    pub fn new(prov: Arc<dyn ReportsProvider>) -> Self {
        Self { prov }
    }
}

// #[async_trait]
// impl Provider for ProviderImpl {
//     async fn create(
//         &self,
//         ctx: Context,
//         organization_id: u64,
//         project_id: u64,
//         request: CreateReportRequest,
//     ) -> Result<Report, PlatformError> {
//         ctx.check_project_permission(
//             organization_id,
//             project_id,
//             ProjectPermission::ManageReports,
//         )?;

//         let report = self
//             .prov
//             .create(
//                 organization_id,
//                 project_id,
//                 metadata::reports::CreateReportRequest {
//                     created_by: ctx.account_id.unwrap(),
//                     tags: request.tags,
//                     name: request.name,
//                     description: request.description,
//                     typ: request.typ,
//                     query: request.query,
//                 },
//             )
//             .await?;

//         report.try_into()
//     }
// }
