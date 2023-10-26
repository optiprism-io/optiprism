use crate::error::Result;
use crate::Destination;
use crate::RequestContext;
use crate::Track;

pub struct Debug {}

impl Debug {
    pub fn new() -> Self {
        Self {}
    }
}

impl Destination<Track> for Debug {
    fn send(&self, ctx: &RequestContext, req: Track) -> Result<()> {
        println!("track: {:?}", req);
        Ok(())
    }
}
