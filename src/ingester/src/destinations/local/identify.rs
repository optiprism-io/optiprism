use std::sync::Arc;

use metadata::dictionaries;
use store::db::OptiDBImpl;

use crate::error::Result;
use crate::Destination;
use crate::Identify;
use crate::RequestContext;
use crate::Track;

pub struct Local {
    db: Arc<OptiDBImpl>,
    dict: Arc<dyn dictionaries::Provider>,
}

impl Local {
    pub fn new(db: Arc<OptiDBImpl>, dict: Arc<dyn dictionaries::Provider>) -> Self {
        Self { db, dict }
    }
}

impl Destination<Identify> for Local {
    fn send(&self, ctx: &RequestContext, req: Identify) -> Result<()> {
        println!("identify: {:?}", req);
        Ok(())
    }
}
