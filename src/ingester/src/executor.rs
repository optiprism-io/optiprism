use std::sync::Arc;

use chrono::Utc;

use crate::destination::Destination;
use crate::error::Result;
use crate::processor::Processor;
use crate::track::Track;
use crate::AppContext;

pub struct Executor {
    processors: Vec<Arc<dyn Processor>>,
    sinks: Vec<Arc<dyn Destination>>,
}

impl Executor {
    pub fn new(processors: Vec<Arc<dyn Processor>>, sinks: Vec<Arc<dyn Destination>>) -> Self {
        Self { processors, sinks }
    }

    pub fn track(&mut self, token: String, mut track: Track) -> Result<()> {
        let ctx = AppContext {
            project_id: 1,
            organization_id: 1,
        };

        for processor in &mut self.processors {
            track = processor.track(&ctx, track)?;
        }

        for sink in &mut self.sinks {
            sink.track(&ctx, track.clone())?;
        }
        Ok(())
    }
}
