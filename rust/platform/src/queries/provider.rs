use crate::data_table::DataTable;
use crate::queries::event_segmentation::EventSegmentation;
use crate::queries::property_values;
use crate::queries::property_values::PropertyValues;
use crate::Context;
use crate::Result;
use std::sync::Arc;
use common::rbac::ProjectPermission;

pub struct QueryProvider {
    query: Arc<query::QueryProvider>,
}

impl QueryProvider {
    pub fn new(query: Arc<query::QueryProvider>) -> Self {
        Self { query }
    }

    pub async fn event_segmentation(
        &self,
        ctx: Context,
        organization_id: u64,
        project_id: u64,
        req: EventSegmentation,
    ) -> Result<DataTable> {
        ctx.check_project_permission(organization_id, project_id, ProjectPermission::ExploreReports)?;

        let lreq = req.try_into()?;
        let result = self
            .query
            .event_segmentation(query::Context::new(organization_id, project_id), lreq)
            .await?;

        result.try_into()
    }

    pub async fn property_values(
        &self,
        ctx: Context,
        organization_id: u64,
        project_id: u64,
        req: PropertyValues,
    ) -> Result<property_values::ListResponse> {
        ctx.check_project_permission(organization_id, project_id, ProjectPermission::ExploreReports)?;

        let lreq = req.try_into()?;
        let result = self
            .query
            .property_values(query::Context::new(organization_id, project_id), lreq)
            .await?;

        result.try_into()
    }
}
