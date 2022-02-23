use crate::Result;
use metadata::{events_queue::EventWithContext, Metadata};
use std::sync::Arc;

pub struct Provider {
    metadata: Arc<Metadata>,
}

impl Provider {
    pub fn new(metadata: Arc<Metadata>) -> Self {
        Self { metadata }
    }

    pub async fn ingest(&self, source: EventWithContext) -> Result<()> {
        self.metadata.events_queue.push(source).await?;
        Ok(())
    }
}
