use std::sync::Arc;

use crate::error::Result;
use crate::processor::Processor;
use crate::sink::Sink;
use crate::track::Track;
use crate::Context;

pub struct Executor {
    processors: Vec<Arc<dyn Processor>>,
    sinks: Vec<Arc<dyn Sink>>,
}

impl Executor {
    pub fn new(processors: Vec<Arc<dyn Processor>>, sinks: Vec<Arc<dyn Sink>>) -> Self {
        Self { processors, sinks }
    }

    pub fn execute(&mut self, token: String, mut track: Track) -> Result<()> {
        let ctx = Context {
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
