use super::Processor;
use crate::Result;
use metadata::Metadata;
use std::sync::Arc;

pub struct Provider {
    metadata: Arc<Metadata>,
    processors: Vec<Box<dyn Processor>>,
}

impl Provider {
    pub fn new(metadata: Arc<Metadata>, processors: Vec<Box<dyn Processor>>) -> Self {
        Self {
            metadata,
            processors,
        }
    }

    pub async fn watch(&self) -> Result<()> {
        loop {
            let mut source = self.metadata.events_queue.poll_next().await?;
            let event = self
                .metadata
                .events
                .get_by_name(source.project_id, &source.event.name)
                .await?;
            for processor in self.processors.iter() {
                if let Some(event) = processor.process(&source.context, &source.event)? {
                    source.event = event
                }
            }
            if let Some(properties) = &source.event.properties {
                for (k, v) in properties {
                    unimplemented!()
                }
            }
        }
        Ok(())
    }
}
