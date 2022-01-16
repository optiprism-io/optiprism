use crate::store::store::Store;
use crate::Result;
use crate::{accounts, events};
use std::sync::Arc;

pub struct Metadata {
    pub events: events::Provider,
    pub accounts: accounts::Provider,
}

impl Metadata {
    pub fn try_new(store: Arc<Store>) -> Result<Self> {
        Ok(Metadata {
            events: events::Provider::try_new(store.clone())?,
            accounts: accounts::Provider::new(store.clone()),
        })
    }
}
