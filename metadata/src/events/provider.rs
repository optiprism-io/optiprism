use crate::error::Error;
use crate::store::store::{Namespace, Store};
use crate::Result;
use std::collections::HashMap;

use bincode::{deserialize, serialize};
use chrono::Utc;
use std::sync::Arc;
use types::event::Event;

const KV_NAMESPACE: Namespace = Namespace::Events;

pub struct Provider {
    store: Arc<Store>,
    name_idx: HashMap<String, u64>,
}

impl Provider {
    pub fn try_new(kv: Arc<Store>) -> Result<Self> {
        let prov = Provider {
            store: kv.clone(),
            name_idx: HashMap::new(),
        };
        prov.init()?;
        Ok(prov)
    }

    fn init(&self) -> Result<()> {
        Ok(())
    }

    fn update_idx(&mut self, event: &Event, prev_event: Option<&Event>) {
        if let Some(e) = prev_event {
            if e.name != event.name {
                self.name_idx.remove(&e.name);
            }
        }
        self.name_idx.insert(event.name.clone(), event.id);
    }

    pub async fn create_event(&mut self, event: Event) -> Result<Event> {
        // name is unique among all events
        match self.name_idx.get(&event.name) {
            Some(_) => return Err(Error::EventWithSameNameAlreadyExist),
            None => {}
        }

        let mut event = event.clone();
        event.created_at = Some(Utc::now());
        event.id = self.store.next_seq(KV_NAMESPACE).await?;
        self.store
            .put(KV_NAMESPACE, event.id.to_le_bytes(), serialize(&event)?)
            .await?;
        self.update_idx(&event, None);

        Ok(event)
    }

    pub async fn update_event(&mut self, event: Event) -> Result<Event> {
        let prev_event = self.get_event_by_id(event.id).await?;

        let mut event = event.clone();
        event.updated_at = Some(Utc::now());

        self.store
            .put(KV_NAMESPACE, event.id.to_le_bytes(), serialize(&event)?)
            .await?;

        self.update_idx(&event, Some(&prev_event));
        Ok(event)
    }

    pub async fn get_event_by_id(&self, id: u64) -> Result<Event> {
        match self.store.get(KV_NAMESPACE, id.to_le_bytes()).await? {
            None => Err(Error::EventDoesNotExist),
            Some(value) => Ok(deserialize(&value)?),
        }
    }

    pub async fn get_event_by_name(&self, name: &str) -> Result<Event> {
        match self.name_idx.get(name) {
            None => Err(Error::EventDoesNotExist),
            Some(id) => self.get_event_by_id(id.clone()).await,
        }
    }

    pub async fn delete_event(&mut self, id: u64) -> Result<Event> {
        let event = self.get_event_by_id(id).await?;
        self.store.delete(KV_NAMESPACE, id.to_le_bytes()).await?;

        self.name_idx.remove(&event.name);
        Ok(event)
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
