use std::sync::Arc;

use serde::Deserialize;
use serde::Serialize;

use crate::accounts;
use crate::custom_events;
use crate::custom_properties;
use crate::dashboards;
use crate::database;
use crate::dictionaries;
use crate::events;
use crate::organizations;
use crate::projects;
use crate::properties;
use crate::reports;
use crate::store::Store;
use crate::stub;
use crate::Result;

pub struct MetadataProvider {
    pub dashboards: Arc<dyn dashboards::Provider>,
    pub reports: Arc<dyn reports::Provider>,
    pub events: Arc<dyn events::Provider>,
    pub custom_events: Arc<dyn custom_events::Provider>,
    pub event_properties: Arc<dyn properties::Provider>,
    pub user_properties: Arc<dyn properties::Provider>,
    pub custom_properties: Arc<dyn custom_properties::Provider>,
    pub organizations: Arc<dyn organizations::Provider>,
    pub projects: Arc<dyn projects::Provider>,
    pub accounts: Arc<dyn accounts::Provider>,
    pub database: Arc<dyn database::Provider>,
    pub dictionaries: Arc<dyn dictionaries::Provider>,
}

impl MetadataProvider {
    pub fn try_new(store: Arc<Store>) -> Result<Self> {
        let events = Arc::new(events::ProviderImpl::new(store.clone()));
        Ok(MetadataProvider {
            dashboards: Arc::new(dashboards::ProviderImpl::new(store.clone())),
            reports: Arc::new(reports::ProviderImpl::new(store.clone())),
            events: events.clone(),
            custom_events: Arc::new(custom_events::ProviderImpl::new(store.clone(), events)),
            event_properties: Arc::new(properties::ProviderImpl::new_event(store.clone())),
            user_properties: Arc::new(properties::ProviderImpl::new_user(store.clone())),
            custom_properties: Arc::new(custom_properties::ProviderImpl::new(store.clone())),
            organizations: Arc::new(organizations::ProviderImpl::new(store.clone())),
            projects: Arc::new(projects::ProviderImpl::new(store.clone())),
            accounts: Arc::new(accounts::ProviderImpl::new(store.clone())),
            database: Arc::new(database::ProviderImpl::new(store.clone())),
            dictionaries: Arc::new(dictionaries::ProviderImpl::new(store)),
        })
    }

    pub fn new_stub() -> Self {
        MetadataProvider {
            dashboards: Arc::new(stub::Dashboards {}),
            reports: Arc::new(stub::Reports {}),
            events: Arc::new(stub::Events {}),
            custom_events: Arc::new(stub::CustomEvents {}),
            event_properties: Arc::new(stub::Properties {}),
            user_properties: Arc::new(stub::Properties {}),
            custom_properties: Arc::new(stub::CustomProperties {}),
            organizations: Arc::new(stub::Organizations {}),
            projects: Arc::new(stub::Projects {}),
            accounts: Arc::new(stub::Accounts {}),
            database: Arc::new(stub::Database {}),
            dictionaries: Arc::new(stub::Dictionaries {}),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct ResponseMetadata {
    pub next: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub struct ListResponse<T> {
    pub data: Vec<T>,
    pub meta: ResponseMetadata,
}
