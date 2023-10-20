use crate::error::Result;
use crate::AppContext;
use crate::Destination;
use crate::Track;

pub struct Debug {}

impl Debug {
    pub fn new() -> Self {
        Self {}
    }
}

impl Destination<Track> for Debug {
    fn send(&self, ctx: &AppContext, req: Track) -> Result<()> {
        println!("track: {:?}", req);
        Ok(())
    }
}
