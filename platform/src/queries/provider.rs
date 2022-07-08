use crate::Context;
use crate::Result;
use std::sync::Arc;
use crate::data_table::DataTable;
use crate::queries::event_segmentation::EventSegmentation;
use crate::queries::property_values;
use crate::queries::property_values::PropertyValues;

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
        _organization_id: u64,
        project_id: u64,
        req: EventSegmentation,
    ) -> Result<DataTable> {
        let lreq = req.try_into()?;
        let result = self
            .query
            .event_segmentation(ctx.into_query_context(project_id), lreq)
            .await?;
        Ok(result.try_into()?)
    }

    pub async fn property_values(
        &self,
        ctx: Context,
        _organization_id: u64,
        project_id: u64,
        req: PropertyValues,
    ) -> Result<property_values::ListResponse> {
        let lreq = req.try_into()?;
        let result = self
            .query
            .property_values(ctx.into_query_context(project_id), lreq)
            .await?;
        Ok(result.try_into()?)
    }
}