use super::Event;
use crate::error::Result;
use bincode::{deserialize, serialize};
use chrono::{Utc};
use std::sync::Arc;
use common::kv::SeqKV;
use crate::event::Provider;


pub struct ProviderImpl {
    store: Arc<dyn SeqKV>,
}

impl ProviderImpl {
    pub fn new(store: Arc<dyn SeqKV>) -> Self {
        Self {
            store: store.clone(),
        }
    }
}

impl Provider for ProviderImpl {
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
