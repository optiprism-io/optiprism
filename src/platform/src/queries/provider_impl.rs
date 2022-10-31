use std::sync::Arc;

use axum::async_trait;
use common::rbac::ProjectPermission;

use crate::queries::event_segmentation::EventSegmentation;
use crate::queries::property_values;
use crate::queries::property_values::PropertyValues;
use crate::queries::Provider;
use crate::Context;
use crate::DataTable;
use crate::Result;

pub struct ProviderImpl {
    query: Arc<dyn query::Provider>,
}

impl ProviderImpl {
    pub fn new(query: Arc<dyn query::Provider>) -> Self {
        Self { query }
    }
}
#[async_trait]
impl Provider for ProviderImpl {
    async fn event_segmentation(
        &self,
        ctx: Context,
        organization_id: u64,
        project_id: u64,
        req: EventSegmentation,
    ) -> Result<DataTable> {
        ctx.check_project_permission(
            organization_id,
            project_id,
            ProjectPermission::ExploreReports,
        )?;

        let lreq = req.try_into()?;
        let result = self
            .query
            .event_segmentation(query::Context::new(organization_id, project_id), lreq)
            .await?;

        result.try_into()
    }

    async fn property_values(
        &self,
        ctx: Context,
        organization_id: u64,
        project_id: u64,
        req: PropertyValues,
    ) -> Result<property_values::ListResponse> {
        ctx.check_project_permission(
            organization_id,
            project_id,
            ProjectPermission::ExploreReports,
        )?;

        let lreq = req.try_into()?;
        let result = self
            .query
            .property_values(query::Context::new(organization_id, project_id), lreq)
            .await?;

        result.try_into()
    }
}
