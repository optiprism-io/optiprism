use crate::store::store::Store;
use crate::{accounts, events};
use std::sync::Arc;

pub struct Metadata {
    pub events: events::Provider,
    pub accounts: accounts::Provider,
}

impl Metadata {
    pub fn new(store: Arc<Store>) -> Self {
        Metadata {
            events: events::Provider::new(store.clone()),
            accounts: accounts::Provider::new(store.clone()),
        }
    }
}
