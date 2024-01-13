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
    pub system_properties: Arc<dyn properties::Provider>,
    pub custom_properties: Arc<dyn custom_properties::Provider>,
    pub organizations: Arc<dyn organizations::Provider>,
    pub projects: Arc<dyn projects::Provider>,
    pub accounts: Arc<dyn accounts::Provider>,
    pub dictionaries: Arc<dyn dictionaries::Provider>,
}

impl MetadataProvider {
    pub fn try_new(db: Arc<TransactionDB>, opti_db: Arc<OptiDBImpl>) -> Result<Self> {
        let events = Arc::new(events::Events::new(db.clone()));
        Ok(MetadataProvider {
            dashboards: Arc::new(dashboards::Dashboards::new(db.clone())),
            reports: Arc::new(reports::Reports::new(db.clone())),
            events: events.clone(),
            custom_events: Arc::new(custom_events::CustomEvents::new(db.clone(), events)),
            event_properties: Arc::new(properties::Properties::new_event(
                db.clone(),
                opti_db.clone(),
            )),
            user_properties: Arc::new(properties::Properties::new_user(
                db.clone(),
                opti_db.clone(),
            )),
            system_properties: Arc::new(properties::Properties::new_system(
                db.clone(),
                opti_db.clone(),
            )),
            custom_properties: Arc::new(custom_properties::ProviderImpl::new(db.clone())),
            organizations: Arc::new(organizations::Organizations::new(db.clone())),
            projects: Arc::new(projects::Projects::new(db.clone())),
            accounts: Arc::new(accounts::Accounts::new(db.clone())),
            dictionaries: Arc::new(dictionaries::Dictionaries::new(db)),
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
            system_properties: Arc::new(stub::Properties {}),
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
