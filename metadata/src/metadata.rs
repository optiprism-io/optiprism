use crate::store::store::Store;
use crate::{event_properties, Result};
use crate::{accounts, events};
use std::sync::Arc;

pub struct Metadata {
    pub events: events::Provider,
    pub event_properties: event_properties::Provider,
    pub accounts: accounts::Provider,
}

impl Metadata {
    pub fn try_new(store: Arc<Store>) -> Result<Self> {
        Ok(Metadata {
            events: events::Provider::new(store.clone()),
            event_properties: event_properties::Provider::new(store.clone()),
            accounts: accounts::Provider::new(store),
        })
    }
}
