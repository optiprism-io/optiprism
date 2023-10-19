use crate::destination::Destination;
use crate::error::Result;
use crate::track;
use crate::AppContext;

pub struct DebugSink {}

impl DebugSink {
    pub fn new() -> Self {
        Self {}
    }
}

impl Destination for DebugSink {
    fn track(&self, ctx: &AppContext, track: track::Track) -> Result<()> {
        println!("track: {:?}", track);
        Ok(())
    }
}
