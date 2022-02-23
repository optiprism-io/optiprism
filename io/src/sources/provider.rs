use crate::{types::EventWithContext, Result};
use metadata::Metadata;
use std::sync::Arc;

pub struct Provider {
    metadata: Arc<Metadata>,
}

impl Provider {
    pub fn new(metadata: Arc<Metadata>) -> Self {
        Self { metadata }
    }

    pub async fn ingest(&self, event: EventWithContext) -> Result<()> {
        self.metadata.events_queue.push(event).await?;
        Ok(())
    }
}
