use std::sync::Arc;

use rocksdb::TransactionDB;
use serde::Deserialize;
use serde::Serialize;
use store::db::OptiDBImpl;

use crate::accounts;
use crate::accounts::Accounts;
use crate::custom_events;
use crate::custom_events::CustomEvents;
use crate::dashboards;
use crate::dashboards::Dashboards;
use crate::dictionaries;
use crate::dictionaries::Dictionaries;
use crate::events;
use crate::events::Events;
use crate::organizations;
use crate::organizations::Organizations;
use crate::projects;
use crate::projects::Projects;
use crate::properties;
use crate::properties::Properties;
use crate::reports;
use crate::reports::Reports;
use crate::sessions;
use crate::sessions::Sessions;
use crate::Result;

pub struct MetadataProvider {
    pub dashboards: Arc<Dashboards>,
    pub reports: Arc<Reports>,
    pub events: Arc<Events>,
    pub custom_events: Arc<CustomEvents>,
    pub event_properties: Arc<Properties>,
    pub user_properties: Arc<Properties>,
    pub system_properties: Arc<Properties>,
    pub organizations: Arc<Organizations>,
    pub projects: Arc<Projects>,
    pub accounts: Arc<Accounts>,
    pub dictionaries: Arc<Dictionaries>,
    pub sessions: Arc<Sessions>,
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
            organizations: Arc::new(organizations::Organizations::new(db.clone())),
            projects: Arc::new(projects::Projects::new(db.clone())),
            accounts: Arc::new(accounts::Accounts::new(db.clone())),
            dictionaries: Arc::new(dictionaries::Dictionaries::new(db.clone())),
            sessions: Arc::new(sessions::Sessions::new(db)),
        })
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
