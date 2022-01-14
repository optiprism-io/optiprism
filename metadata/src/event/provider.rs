use std::sync::Arc;
use crate::{
    Result,
};
use bincode::{deserialize, serialize};
use chrono::{DateTime, Utc};
use crate::event::types::Event;
use crate::kv::kv::{KV, Table};

const KV_TABLE: Table = Table::Events;

pub struct Provider {
    kv: Arc<KV>,
}

impl Provider {
    pub fn new(kv: Arc<KV>) -> Self {
        Provider { kv: kv.clone() }
    }

    pub async fn create_event(&self, event: Event) -> Result<Event> {
        let mut event = event.clone();
        event.created_at = Some(Utc::now());
        event.id = self.kv.next_seq(KV_TABLE).await?;
        self.kv
            .put_entity(
                KV_TABLE,
                event.id,
                &event,
            )
            .await?;
        Ok(event)
    }

    pub async fn update_event(&self, event: Event) -> Result<Event> {
        let mut event = event.clone();
        event.updated_at = Some(Utc::now());
        self.kv
            .put_entity(
                KV_TABLE,
                event.id,
                &event,
            )
            .await?;
        Ok(event)
    }

    pub async fn get_event(&self, id: u64) -> Result<Option<Event>> {
        Ok(
            match self.kv.get(KV_TABLE, id.to_le_bytes().as_ref()).await? {
                None => None,
                Some(value) => Some(deserialize(&value)?),
            },
        )
    }

    pub async fn delete_event(&self, id: u64) -> Result<()> {
        Ok(self.kv.delete(KV_TABLE, id.to_le_bytes().as_ref()).await?)
    }

    pub async fn list_events(&self) -> Result<Vec<Event>> {
        let list = self
            .kv
            .list(KV_TABLE)
            .await?
            .iter()
            .map(|v| deserialize(v.1.as_ref()))
            .collect::<bincode::Result<_>>()?;

        Ok(list)
    }
}
