use std::fmt::Debug;
use std::sync::Arc;

use rocksdb::TransactionDB;
use serde::Deserialize;
use serde::Serialize;
use storage::db::OptiDBImpl;

use crate::{accounts, bookmarks, config};
use crate::accounts::Accounts;
use crate::backups::Backups;
use crate::bookmarks::Bookmarks;
use crate::config::Config;
use crate::custom_events;
use crate::custom_events::CustomEvents;
use crate::dashboards;
use crate::dashboards::Dashboards;
use crate::dictionaries;
use crate::dictionaries::Dictionaries;
use crate::events;
use crate::events::Events;
use crate::groups::Groups;
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
    pub config: Arc<Config>,
    pub dashboards: Arc<Dashboards>,
    pub reports: Arc<Reports>,
    pub bookmarks: Arc<Bookmarks>,
    pub events: Arc<Events>,
    pub custom_events: Arc<CustomEvents>,
    pub properties: Arc<Properties>,
    pub event_properties: Arc<Properties>,
    pub group_properties: Vec<Arc<Properties>>,
    pub organizations: Arc<Organizations>,
    pub projects: Arc<Projects>,
    pub accounts: Arc<Accounts>,
    pub dictionaries: Arc<Dictionaries>,
    pub sessions: Arc<Sessions>,
    pub groups: Arc<Groups>,
    pub backups: Arc<Backups>,
}

impl MetadataProvider {
    pub fn try_new(db: Arc<TransactionDB>, opti_db: Arc<OptiDBImpl>) -> Result<Self> {
        let dicts = Arc::new(Dictionaries::new(db.clone()));
        let events = Arc::new(events::Events::new(db.clone(), dicts.clone()));
        let accounts = Arc::new(accounts::Accounts::new(db.clone()));
        Ok(MetadataProvider {
            config: Arc::new(config::Config::new(db.clone())),
            dashboards: Arc::new(dashboards::Dashboards::new(db.clone())),
            reports: Arc::new(reports::Reports::new(db.clone())),
            bookmarks: Arc::new(bookmarks::Bookmarks::new(db.clone())),
            events: events.clone(),
            custom_events: Arc::new(custom_events::CustomEvents::new(db.clone(), events)),
            properties: Arc::new(properties::Properties::new(
                db.clone(),
                opti_db.clone(),
            )),
            event_properties: Arc::new(properties::Properties::new_event(
                db.clone(),
                opti_db.clone(),
            )),
            group_properties: properties::Properties::new_group(db.clone(), opti_db.clone()),
            organizations: Arc::new(organizations::Organizations::new(db.clone(), accounts.clone())),
            projects: Arc::new(projects::Projects::new(db.clone())),
            accounts: accounts.clone(),
            dictionaries: dicts.clone(),
            sessions: Arc::new(sessions::Sessions::new(db.clone())),
            groups: Arc::new(Groups::new(db.clone())),
            backups: Arc::new(Backups::new(db)),
        })
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ResponseMetadata {
    pub next: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ListResponse<T>
where
    T: Debug,
{
    pub data: Vec<T>,
    pub meta: ResponseMetadata,
}

impl<T> ListResponse<T>
where
    T: Debug,
{
    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

impl<T> IntoIterator for ListResponse<T>
where
    T: Debug,
{
    type Item = T;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.data.into_iter()
    }
}
