use std::sync::Arc;
use query::QueryProvider;
use crate::event_segmentation::types::EventSegmentation;
use crate::Context;
use crate::event_segmentation::result::Series;
use crate::Result;

pub struct Provider {
    query: Arc<QueryProvider>,
}

impl Provider {
    pub fn new(query: Arc<QueryProvider>) -> Self {
        Self { query }
    }

    pub async fn event_segmentation(&self, ctx: Context, req: EventSegmentation) -> Result<Series> {
        let result = self.query.event_segmentation(ctx.into(),req.try_into()?).await?;
        Ok(result.try_into()?)
    }
}