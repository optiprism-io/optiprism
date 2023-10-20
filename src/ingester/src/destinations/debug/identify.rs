use crate::error::Result;
use crate::AppContext;
use crate::Destination;
use crate::Identify;
use crate::Track;

pub struct Debug {}

impl Debug {
    pub fn new() -> Self {
        Self {}
    }
}

impl Destination<Identify> for Debug {
    fn send(&self, ctx: &AppContext, req: Identify) -> Result<()> {
        println!("identify: {:?}", req);
        Ok(())
    }
}
