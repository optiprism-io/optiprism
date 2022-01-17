use crate::error::Error;
use crate::store::store::{Namespace, Store};
use crate::Result;
use std::collections::HashMap;

use bincode::{deserialize, serialize};
use chrono::Utc;
use std::sync::Arc;
use types::event::Event;

const KV_NAMESPACE: Namespace = Namespace::Events;

type NameKey = (u64, String);

enum IndexOp<'a> {
    Insert(&'a Event),
    Update {
        event: &'a Event,
        prev_event: &'a Event,
    },
    Delete(&'a Event),
}

pub struct Provider {
    store: Arc<Store>,
    name_idx: HashMap<NameKey, u64>,
}

fn name_key(event: &Event) -> NameKey {
    (event.project_id, event.name.clone())
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

    fn update_indices(&mut self, op: IndexOp) {
        match op {
            IndexOp::Insert(event) => {
                self.name_idx.insert(name_key(event), event.id);
            }
            IndexOp::Update { event, prev_event } => {
                if event.name != prev_event.name {
                    self.name_idx.remove(&name_key(prev_event));
                }

                self.name_idx.insert(name_key(event), event.id);
            }
            IndexOp::Delete(event) => {
                self.name_idx.remove(&name_key(event));
            }
        }
    }

    fn check_constraints(&mut self, op: IndexOp) -> Result<()> {
        match op {
            IndexOp::Insert(event) => {
                if let Some(_) = self.name_idx.get(&name_key(event)) {
                    return Err(Error::EventWithSameNameAlreadyExist);
                }
            }
            IndexOp::Update { event, prev_event } => {
                if event.name != prev_event.name {
                    if let Some(_) = self.name_idx.get(&name_key(event)) {
                        return Err(Error::EventWithSameNameAlreadyExist);
                    }
                }
            }
            _ => unreachable!(),
        }

        Ok(())
    }

    pub async fn create_event(&mut self, event: Event) -> Result<Event> {
        self.check_constraints(IndexOp::Insert(&event))?;

        let mut event = event.clone();
        event.created_at = Some(Utc::now());
        event.id = self.store.next_seq(KV_NAMESPACE).await?;
        self.store
            .put(KV_NAMESPACE, event.id.to_le_bytes(), serialize(&event)?)
            .await?;

        self.update_indices(IndexOp::Insert(&event));
        Ok(event)
    }

    pub async fn update_event(&mut self, event: Event) -> Result<Event> {
        let prev_event = self.get_event_by_id(event.id).await?;
        self.check_constraints(IndexOp::Update { event: &event, prev_event: &prev_event })?;

        let mut event = event.clone();
        event.updated_at = Some(Utc::now());

        self.store
            .put(KV_NAMESPACE, event.id.to_le_bytes(), serialize(&event)?)
            .await?;

        self.update_indices(IndexOp::Update { event: &event, prev_event: &prev_event });
        Ok(event)
    }

    pub async fn get_event_by_id(&self, id: u64) -> Result<Event> {
        match self.store.get(KV_NAMESPACE, id.to_le_bytes()).await? {
            None => Err(Error::EventDoesNotExist),
            Some(value) => Ok(deserialize(&value)?),
        }
    }

    pub async fn get_event_by_name(&self, project_id: u64, name: &str) -> Result<Event> {
        match self.name_idx.get(&(project_id, name.to_string())) {
            None => Err(Error::EventDoesNotExist),
            Some(id) => self.get_event_by_id(id.clone()).await,
        }
    }

    pub async fn delete_event(&mut self, id: u64) -> Result<Event> {
        let event = self.get_event_by_id(id).await?;
        self.store.delete(KV_NAMESPACE, id.to_le_bytes()).await?;

        self.update_indices(IndexOp::Delete(&event));
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
