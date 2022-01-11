use std::sync::Arc;
use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};
use crate::EventProvider;
use super::error::Result;
use bincode::{deserialize, serialize};
use crate::kv::KV;
use async_trait::async_trait;

#[derive(Serialize, Deserialize, Clone)]
enum Status {
    Enabled,
    Disabled,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Event {
    pub id: u64,
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
    pub created_by: u64,
    pub update_by: u64,
    pub project_id: u64,
    pub is_system: bool,
    pub tags: Vec<String>,
    pub name: String,
    pub display_name: Option<String>,
    pub description: Option<String>,
    pub status: Status,
    pub properties: Option<Vec<u64>>,
    pub custom_properties: Option<Vec<u64>>,
}

pub struct Provider {
    store: Arc<KV>,
}

impl Provider {
    async fn a() {

    }
}

impl EventProvider for Provider {
    async fn create_event(&self, event: Event) -> Result<Event> {
        let mut w = event.clone();
        w.created_at = Some(Utc::now());
        let wb = serialize(&w)?;
        w.id = self.store.insert(&wb, None).await?;

        Ok(w)
    }

    async fn update_event(&self, event: Event) -> Result<Event> {
        let mut w = event.clone();
        w.updated_at = Some(Utc::now());
        let wb = serialize(&w)?;
        w.id = self.store.update(event.id, &wb, None).await?;

        Ok(w)
    }

    async fn get_event(&self, id: u64) -> Result<Option<Event>> {
        Ok(match self.store.get(id).await? {
            None => None,
            Some(v) => Some(deserialize(&v)?)
        })
    }

    async fn delete_event(&self, id: u64) -> Result<()> {
        Ok(self.store.delete(id).await?)
    }

    async fn list_events(&self) -> Result<Vec<Event>> {
        let list = self.store
            .list().await?
            .iter()
            .map(|v| deserialize(&v))
            .collect::<bincode::Result<_>>()?;

        Ok(list)
    }
}