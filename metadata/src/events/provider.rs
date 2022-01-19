use crate::error::Error;
use crate::store::store::{Store};
use crate::Result;
use std::collections::HashMap;

use bincode::{deserialize, serialize};
use serde::{Deserialize, Serialize};
use chrono::Utc;
use std::sync::Arc;
use types::event::Event;
use crate::store::index;

const KV_NAMESPACE: &[u8] = "entities:events".as_bytes();

fn make_key(ns: &[u8], project_id: u64, key: &[u8]) -> Vec<u8> {
    [ns, b":", project_id.to_le_bytes().as_ref(), key].concat()
}

#[derive(Serialize, Deserialize)]
struct NameKey(u64, String);

fn indexes(event: &Event) -> Vec<index::hash_map::IndexKV> {
    vec![
        (
            "name".as_bytes().to_vec(),
            Some((
                serialize(&NameKey(event.project_id, event.name.clone())).unwrap(),
                serialize(&NameKey(event.project_id, event.name.clone())).unwrap(),
            )))
    ]
}

pub struct Provider {
    store: Arc<Store>,
    idx: index::hash_map::HashMap,
}

impl Provider {
    pub fn new(kv: Arc<Store>) -> Self {
        Provider {
            store: kv,
            idx: index::hash_map::HashMap::new(kv.clone()),
        }
    }

    pub async fn create_event(&mut self, event: Event) -> Result<Event> {
        self.idx.check_insert_constraints(indexes(&event)).await?;

        let mut event = event.clone();
        event.created_at = Some(Utc::now());
        event.id = self.store.next_seq(KV_NAMESPACE).await?;
        self.store
            .put(make_key(KV_NAMESPACE, event.project_id, &event.id.to_le_bytes()), serialize(&event)?)
            .await?;

        self.idx.insert(indexes(&event)).await?;
        Ok(event)
    }

    pub async fn update_event(&mut self, event: Event) -> Result<Event> {
        let prev_event = self.get_event_by_id(event.project_id, event.id).await?;
        self.idx.check_update_constraints(indexes(&event), indexes(&prev_event)).await?;

        let mut event = event.clone();
        event.updated_at = Some(Utc::now());

        self.store
            .put(make_key(KV_NAMESPACE, event.project_id, &event.id.to_le_bytes()), serialize(&event)?)
            .await?;

        self.idx.update(indexes(&event), indexes(&prev_event)).await?;
        Ok(event)
    }

    pub async fn get_event_by_id(&self, project_id: u64, id: u64) -> Result<Event> {
        match self.store.get(make_key(KV_NAMESPACE, project_id, id.to_le_bytes().as_ref())).await? {
            None => Err(Error::EventDoesNotExist),
            Some(value) => Ok(deserialize(&value)?),
        }
    }

    pub async fn get_event_by_name(&self, project_id: u64, name: &str) -> Result<Event> {
        let id = self.idx.get("name".as_bytes(), make_key(KV_NAMESPACE, project_id, name.as_bytes())).await?;
        self.get_event_by_id(u64::from_le_bytes(id.try_into()?)).await
    }

    pub async fn delete_event(&mut self, project_id: u64, id: u64) -> Result<Event> {
        let event = self.get_event_by_id(project_id, id).await?;
        self.store.delete(make_key(KV_NAMESPACE, project_id, id.to_le_bytes().as_ref())).await?;

        self.idx.delete(indexes(&event)).await?;
        Ok(event)
    }

    pub async fn list_events(&self) -> Result<Vec<Event>> {
        let list = self
            .store
            .list_prefix(KV_NAMESPACE)
            .await?
            .iter()
            .map(|v| deserialize(v.1.as_ref()))
            .collect::<bincode::Result<_>>()?;

        Ok(list)
    }
}
