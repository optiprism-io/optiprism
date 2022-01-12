use super::error::Result;
use crate::kv::KV;
use crate::{EventProvider, kv};
use async_trait::async_trait;
use bincode::{deserialize, serialize};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

const KV_TABLE: kv::Table = kv::Table::Events;

#[derive(Serialize, Deserialize, Clone)]
pub enum Status {
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
    kv: KV,
}


#[async_trait]
impl EventProvider for Provider {
    // TODO: create request struct
    async fn create_event(&self, event: Event) -> Result<Event> {
        let mut event = event.clone();
        event.created_at = Some(Utc::now());
        event.id = self.kv.next_seq(KV_TABLE).await?;
        self.kv
            .put(
                KV_TABLE,
                event.id.to_le_bytes().as_ref(),
                serialize(&event)?.as_ref(),
            )
            .await?;
        Ok(event)
    }

    // TODO: update request struct
    async fn update_event(&self, event: Event) -> Result<Event> {
        let mut event = event.clone();
        event.updated_at = Some(Utc::now());
        self.kv
            .put(
                KV_TABLE,
                event.id.to_le_bytes().as_ref(),
                serialize(&event)?.as_ref(),
            )
            .await?;
        Ok(event)
    }

    async fn get_event(&self, id: u64) -> Result<Option<Event>> {
        Ok(match self.kv.get(KV_TABLE, id.to_le_bytes().as_ref()).await? {
            None => None,
            Some(value) => Some(deserialize(&value)?),
        })
    }

    async fn delete_event(&self, id: u64) -> Result<()> {
        Ok(self.kv.delete(KV_TABLE, id.to_le_bytes().as_ref()).await?)
    }

    async fn list_events(&self) -> Result<Vec<Event>> {
        let list = self
            .kv
            .list()
            .await?
            .iter()
            .map(|v| deserialize(&v))
            .collect::<bincode::Result<_>>()?;

        Ok(list)
    }
}
