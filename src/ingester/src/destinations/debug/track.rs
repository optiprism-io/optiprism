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
impl Default for Debug {
    fn default() -> Self {
        Self::new()
    }
}
impl Destination<Track> for Debug {
    fn send(&self, _ctx: &RequestContext, req: Track) -> Result<()> {
        println!("track: {:?}", req);
        Ok(())
    }
}
