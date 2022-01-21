use crate::{accounts, events, organizations, Result, Store};
use std::sync::Arc;

pub struct Metadata {
    pub events: events::Provider,
    pub organizations: organizations::Provider,
    pub accounts: accounts::Provider,
}

impl Metadata {
    pub fn try_new(store: Arc<Store>) -> Result<Self> {
        Ok(Metadata {
            events: events::Provider::new(store.clone()),
            organizations: organizations::Provider::new(store.clone()),
            accounts: accounts::Provider::new(store),
        })
    }
}
