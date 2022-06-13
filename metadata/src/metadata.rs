use crate::store::store::make_data_key;
use crate::{accounts, database, dictionaries, events, organizations, projects, properties, Result, Store};
use bincode::deserialize;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Serialize, Deserialize)]
pub struct ResponseMetadata {
    pub next: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub struct ListResponse<T> {
    pub data: Vec<T>,
    pub meta: ResponseMetadata,
}

pub async fn list<'a, T>(
    store: Arc<Store>,
    organization_id: u64,
    project_id: u64,
    ns: &[u8],
) -> Result<ListResponse<T>>
    where
        T: DeserializeOwned,
{
    let prefix = make_data_key(organization_id, project_id, ns);

    let list = store
        .list_prefix("")
        .await?
        .iter()
        .filter_map(|x| {
            if x.0.len() < prefix.len() || !prefix.as_slice().cmp(&x.0[..prefix.len()]).is_eq() {
                return None;
            }

            Some(deserialize(x.1.as_ref()))
        })
        .collect::<bincode::Result<_>>()?;

    Ok(ListResponse {
        data: list,
        meta: ResponseMetadata { next: None },
    })
}

pub struct Metadata {
    pub events: Arc<events::Provider>,
    pub event_properties: Arc<properties::Provider>,
    pub user_properties: Arc<properties::Provider>,
    pub organizations: Arc<organizations::Provider>,
    pub projects: Arc<projects::Provider>,
    pub accounts: Arc<accounts::Provider>,
    pub database: Arc<database::Provider>,
    pub dictionaries: Arc<dictionaries::Provider>,
}

impl Metadata {
    pub fn try_new(store: Arc<Store>) -> Result<Self> {
        Ok(Metadata {
            events: Arc::new(events::Provider::new(store.clone())),
            event_properties: Arc::new(properties::Provider::new_event(store.clone())),
            user_properties: Arc::new(properties::Provider::new_user(store.clone())),
            organizations: Arc::new(organizations::Provider::new(store.clone())),
            projects: Arc::new(projects::Provider::new(store.clone())),
            accounts: Arc::new(accounts::Provider::new(store.clone())),
            database: Arc::new(database::Provider::new(store.clone())),
            dictionaries:Arc::new(dictionaries::Provider::new(store.clone()))
        })
    }
}
