use std::sync::Arc;

use rocksdb::TransactionDB;
use serde::Deserialize;
use serde::Serialize;
use store::db::OptiDBImpl;

use crate::accounts;
use crate::custom_events;
use crate::custom_properties;
use crate::dashboards;
use crate::dictionaries;
use crate::events;
use crate::organizations;
use crate::projects;
use crate::properties;
use crate::reports;
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
    pub dictionaries: Arc<dyn dictionaries::Provider>,
}

impl MetadataProvider {
    pub fn try_new(db: Arc<TransactionDB>,optiDb:Arc<OptiDBImpl>) -> Result<Self> {
        let events = Arc::new(events::ProviderImpl::new(db.clone()));
        Ok(MetadataProvider {
            dashboards: Arc::new(dashboards::ProviderImpl::new(db.clone())),
            reports: Arc::new(reports::ProviderImpl::new(db.clone())),
            events: events.clone(),
            custom_events: Arc::new(custom_events::ProviderImpl::new(db.clone(), events)),
            event_properties: Arc::new(properties::ProviderImpl::new_event(db.clone(),optiDb.clone())),
            user_properties: Arc::new(properties::ProviderImpl::new_user(db.clone(),optiDb.clone())),
            custom_properties: Arc::new(custom_properties::ProviderImpl::new(db.clone())),
            organizations: Arc::new(organizations::ProviderImpl::new(db.clone())),
            projects: Arc::new(projects::ProviderImpl::new(db.clone())),
            accounts: Arc::new(accounts::ProviderImpl::new(db.clone())),
            dictionaries: Arc::new(dictionaries::ProviderImpl::new(db)),
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
