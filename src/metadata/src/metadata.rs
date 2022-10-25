use std::sync::Arc;

use serde::Deserialize;
use serde::Serialize;

use crate::accounts;
use crate::custom_events;
use crate::database;
use crate::dictionaries;
use crate::events;
use crate::organizations;
use crate::projects;
use crate::properties;
use crate::store::Store;
use crate::Result;

#[derive(Serialize, Deserialize)]
pub struct ResponseMetadata {
    pub next: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub struct ListResponse<T> {
    pub data: Vec<T>,
    pub meta: ResponseMetadata,
}

pub struct MetadataProvider {
    pub events: Arc<events::Provider>,
    pub custom_events: Arc<custom_events::Provider>,
    pub event_properties: Arc<properties::Provider>,
    pub user_properties: Arc<properties::Provider>,
    pub organizations: Arc<organizations::Provider>,
    pub projects: Arc<projects::Provider>,
    pub accounts: Arc<accounts::Provider>,
    pub database: Arc<database::Provider>,
    pub dictionaries: Arc<dictionaries::Provider>,
}

impl MetadataProvider {
    pub fn try_new(store: Arc<Store>) -> Result<Self> {
        let events = Arc::new(events::Provider::new(store.clone()));
        Ok(MetadataProvider {
            events: events.clone(),
            custom_events: Arc::new(custom_events::Provider::new(store.clone(), events)),
            event_properties: Arc::new(properties::Provider::new_event(store.clone())),
            user_properties: Arc::new(properties::Provider::new_user(store.clone())),
            organizations: Arc::new(organizations::Provider::new(store.clone())),
            projects: Arc::new(projects::Provider::new(store.clone())),
            accounts: Arc::new(accounts::Provider::new(store.clone())),
            database: Arc::new(database::Provider::new(store.clone())),
            dictionaries: Arc::new(dictionaries::Provider::new(store)),
        })
    }
}
