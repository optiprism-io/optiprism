use super::EventWithContext;
use crate::{Result, Store};
use std::sync::Arc;

pub struct Provider {
    store: Arc<Store>,
}

impl Provider {
    pub fn new(store: Arc<Store>) -> Self {
        Provider { store }
    }

    pub async fn push(&self, source: EventWithContext) -> Result<()> {
        unimplemented!()
    }

    pub async fn poll_next(&self) -> Result<EventWithContext> {
        unimplemented!()
    }
}
