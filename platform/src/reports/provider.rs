use std::sync::Arc;
use query::QueryProvider;
use query::reports::event_segmentation::types::EventSegmentation;
use query::reports::results::Series;
use crate::Context;
use crate::Result;

pub struct Provider {
    query: Arc<QueryProvider>,
}

impl Provider {
    pub fn new(query: Arc<QueryProvider>) -> Self {
        Self { query }
    }

    pub async fn event_segmentation(&self, ctx: Context, req: EventSegmentation) -> Result<Series> {
        Ok(self.query.event_segmentation(ctx.into(),req).await?)
    }
}