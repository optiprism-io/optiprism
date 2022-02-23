use crate::Result;
use metadata::Metadata;
use std::sync::Arc;

pub struct Provider {
    metadata: Arc<Metadata>,
}

impl Provider {
    pub fn new(metadata: Arc<Metadata>) -> Self {
        Self { metadata }
    }

    pub async fn watch(&self) -> Result<()> {
        loop {
            let source = self.metadata.events_queue.poll_next().await?;
            let event = self
                .metadata
                .events
                .get_by_name(source.project_id, &source.event)
                .await?;
            if let Some(properties) = &source.properties {
                for (k, v) in properties {
                    let k = k;
                    let v = v;
                }
            }
        }
        Ok(())
    }
}
