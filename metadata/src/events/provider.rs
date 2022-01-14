use crate::store::store::{Namespace, Store};
use crate::Result;
use bincode::{deserialize, serialize};
use chrono::Utc;
use std::sync::Arc;
use types::event::Event;

const KV_NAMESPACE: Namespace = Namespace::Events;

pub struct Provider {
    store: Arc<Store>,
}

impl Provider {
    pub fn new(kv: Arc<Store>) -> Self {
        Provider { store: kv.clone() }
    }

    pub async fn create_event(&self, event: Event) -> Result<Event> {
        let mut event = event.clone();
        event.created_at = Some(Utc::now());
        event.id = self.store.next_seq(KV_NAMESPACE).await?;
        self.store
            .put(KV_NAMESPACE, event.id.to_le_bytes(), serialize(&event)?)
            .await?;
        Ok(event)
    }

    pub async fn update_event(&self, event: Event) -> Result<Option<Event>> {
        let mut event = event.clone();
        event.updated_at = Some(Utc::now());
        Ok(
            match self
                .store
                .put_checked(KV_NAMESPACE, event.id.to_le_bytes(), serialize(&event)?)
                .await?
            {
                None => None,
                Some(_) => Some(event),
            },
        )
    }

    pub async fn get_event(&self, id: u64) -> Result<Option<Event>> {
        Ok(
            match self.store.get(KV_NAMESPACE, id.to_le_bytes()).await? {
                None => None,
                Some(value) => Some(deserialize(&value)?),
            },
        )
    }

    pub async fn delete_event(&self, id: u64) -> Result<Option<Event>> {
        Ok(
            match self
                .store
                .delete_checked(KV_NAMESPACE, id.to_le_bytes())
                .await?
            {
                None => None,
                Some(v) => Some(deserialize(&v)?),
            },
        )
    }

    pub async fn list_events(&self) -> Result<Vec<Event>> {
        let list = self
            .store
            .list(KV_NAMESPACE)
            .await?
            .iter()
            .map(|v| deserialize(v.1.as_ref()))
            .collect::<bincode::Result<_>>()?;

        Ok(list)
    }
}
