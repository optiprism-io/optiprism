use crate::{accounts, event_properties, events, organizations, projects, Result, Store};
use std::sync::Arc;
use serde::Serialize;

#[derive(Serialize)]
pub struct ResponseMetadata {
    pub next: Option<String>,
}

#[derive(Serialize)]
pub struct ListResponse<T> {
    pub data: Vec<T>,
    pub meta: ResponseMetadata,
}

pub struct Metadata {
    pub events: events::Provider,
    pub event_properties: event_properties::Provider,
    pub organizations: organizations::Provider,
    pub projects: projects::Provider,
    pub accounts: accounts::Provider,
}

impl Metadata {
    pub fn try_new(store: Arc<Store>) -> Result<Self> {
        Ok(Metadata {
            events: events::Provider::new(store.clone()),
            event_properties: event_properties::Provider::new(store.clone()),
            organizations: organizations::Provider::new(store.clone()),
            projects: projects::Provider::new(store.clone()),
            accounts: accounts::Provider::new(store),
        })
    }
}
