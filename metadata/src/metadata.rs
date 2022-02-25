use crate::{accounts, event_properties, events, organizations, projects, Result, Store};
use std::sync::Arc;
use bincode::deserialize;
use serde::{Deserialize, Serialize};
use serde::de::DeserializeOwned;
use crate::store::store::make_data_key;

#[derive(Serialize, Deserialize)]
pub struct ResponseMetadata {
    pub next: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub struct ListResponse<T> {
    pub data: Vec<T>,
    pub meta: ResponseMetadata,
}

pub async fn list<'a, T>(store: Arc<Store>, organization_id: u64, project_id: u64, ns: &[u8]) -> Result<ListResponse<T>> where T: DeserializeOwned {
    let prefix = make_data_key(organization_id, project_id, ns);

    let list = store.list_prefix("").await?.iter().filter_map(|x| {
        if x.0.len() < prefix.len() || !prefix.as_slice().cmp(&x.0[..prefix.len()]).is_eq() {
            return None;
        }

        Some(deserialize(x.1.as_ref()))
    }).collect::<bincode::Result<_>>()?;

    Ok(ListResponse {
        data: list,
        meta: ResponseMetadata { next: None },
    })
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
