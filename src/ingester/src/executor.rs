use crate::error::Result;
use crate::processor::Processor;
use crate::sink::Sink;
use crate::track::Track;

pub struct Executor {
    processors: Vec<Box<dyn Processor>>,
    sinks: Vec<Box<dyn Sink>>,
}

impl Executor {
    pub fn new(processors: Vec<Box<dyn Processor>>, sinks: Vec<Box<dyn Sink>>) -> Self {
        Self { processors, sinks }
    }

    pub fn execute(&mut self, mut track: Track) -> Result<()> {
        for processor in &mut self.processors {
            track = processor.track(track)?;
        }

        for sink in &mut self.sinks {
            sink.track(track.clone())?;
        }
        Ok(())
    }
}
