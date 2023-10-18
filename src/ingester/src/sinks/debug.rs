use crate::error::Result;
use crate::sink::Sink;
use crate::track;
use crate::Context;

pub struct DebugSink {}

impl DebugSink {
    pub fn new() -> Self {
        Self {}
    }
}

impl Sink for DebugSink {
    fn track(&self, ctx: &Context, track: track::Track) -> Result<()> {
        println!("track: {:?}", track);
        Ok(())
    }
}
