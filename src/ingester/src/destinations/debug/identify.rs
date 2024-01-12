use crate::error::Result;
use crate::Destination;
use crate::Identify;
use crate::RequestContext;

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

impl Destination<Identify> for Debug {
    fn send(&self, _ctx: &RequestContext, req: Identify) -> Result<()> {
        println!("identify: {:?}", req);
        Ok(())
    }
}
