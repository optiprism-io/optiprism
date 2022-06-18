use std::sync::Arc;
use query::QueryProvider;
use crate::event_segmentation::types::EventSegmentation;
use crate::Context;
use crate::event_segmentation::result::DataTable;
use crate::Result;

pub struct Provider {
    query: Arc<QueryProvider>,
}

impl Provider {
    pub fn new(query: Arc<QueryProvider>) -> Self {
        Self { query }
    }

    pub async fn event_segmentation(&self, ctx: Context, organization_id: u64, project_id: u64, req: EventSegmentation) -> Result<DataTable> {
        let lreq = req.try_into()?;
        let result = self.query.event_segmentation(ctx.into_query_context(project_id), lreq).await?;
        Ok(result.try_into()?)
    }
}