use crate::event_segmentation::result::DataTable;
use crate::event_segmentation::types::EventSegmentation;
use crate::Context;
use crate::Result;
use query::QueryProvider;
use std::sync::Arc;

pub struct Provider {
    query: Arc<QueryProvider>,
}

impl Provider {
    pub fn new(query: Arc<QueryProvider>) -> Self {
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
        result.try_into()
    }
}
