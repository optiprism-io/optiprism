use crate::error::Result;
use crate::Destination;
use crate::Identify;
use crate::RequestContext;

#[derive(Default)]
pub struct Local {}

impl Local {
    pub fn new() -> Self {
        Self {}
    }
}

impl Destination<Identify> for Local {
    fn send(&self, _ctx: &RequestContext, req: Identify) -> Result<()> {
        println!("identify: {:?}", req);
        Ok(())
    }
}
