use std::sync::Arc;

use metadata::dictionaries;
use store::SortedMergeTree;

use crate::error::Result;
use crate::Destination;
use crate::Identify;
use crate::RequestContext;
use crate::Track;

pub struct Debug {
    tbl: Arc<dyn SortedMergeTree>,
    dict: Arc<dyn dictionaries::Provider>,
}

impl Debug {
    pub fn new(tbl: Arc<dyn SortedMergeTree>, dict: Arc<dyn dictionaries::Provider>) -> Self {
        Self { tbl, dict }
    }
}

impl Destination<Identify> for Debug {
    fn send(&self, ctx: &RequestContext, req: Identify) -> Result<()> {
        println!("identify: {:?}", req);
        Ok(())
    }
}
