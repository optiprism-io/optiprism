use crate::error::Error;
use crate::store::store::{Key, Store};
use crate::Result;

use crate::store::index;
use bincode::{deserialize, serialize};
use chrono::Utc;
use std::sync::Arc;
use crate::events::Event;
use crate::events::types::{CreateEventRequest, UpdateEventRequest};


const NAMESPACE: &str = "events";
const IDX_NAME: &str = "name";
const IDX_DISPLAY_NAME: &str = "display_name";

fn index_keys(project_id: u64, name: &str, display_name: &Option<String>) -> Vec<Option<Vec<u8>>> {
    if let Some(display_name) = display_name {
        vec![
            Some(Key::Index(NAMESPACE, project_id, IDX_DISPLAY_NAME, display_name).as_bytes()),
            None,
        ]
    } else {
        vec![
            Some(Key::Index(NAMESPACE, project_id, IDX_NAME, name).as_bytes()),
            None,
        ]
    }
}

pub struct Provider {
    store: Arc<Store>,
    idx: index::hash_map::HashMap,
}

impl Provider {
    pub fn new(kv: Arc<Store>) -> Self {
        Provider {
            store: kv.clone(),
            idx: index::hash_map::HashMap::new(kv),
        }
    }

    pub async fn create_event(&mut self, req: CreateEventRequest) -> Result<Event> {
        let idx_keys = index_keys(req.project_id, req.name.as_str(), &req.display_name);
        self.idx
            .check_insert_constraints(idx_keys.as_ref())
            .await?;

        let created_at = Utc::now();
        let id = self.store.next_seq(Key::IdSequence(NAMESPACE, req.project_id).as_bytes()).await?;

        let event = req.into_event(id, created_at);
        self.store
            .put(
                Key::Data(NAMESPACE, event.project_id, event.id).as_bytes(),
                serialize(&event)?,
            )
            .await?;

        self.idx.insert(idx_keys.as_ref(), event.id.to_le_bytes()).await?;
        Ok(event)
    }

    pub async fn get_event_by_id(&self, project_id: u64, id: u64) -> Result<Event> {
        match self.store.get(Key::Data(NAMESPACE, project_id, id).as_bytes()).await? {
            None => Err(Error::EventDoesNotExist),
            Some(value) => Ok(deserialize(&value)?),
        }
    }

    pub async fn get_event_by_name(&self, project_id: u64, name: &str) -> Result<Event> {
        let id = self
            .idx
            .get(Key::Index(NAMESPACE, project_id, IDX_NAME, name).as_bytes())
            .await?;
        self.get_event_by_id(project_id, u64::from_le_bytes(id.try_into()?))
            .await
    }

    pub async fn list_events(&self) -> Result<Vec<Event>> {
        let list = self
            .store
            .list_prefix(b"/events/ent") // TODO doesn't work
            .await?
            .iter()
            .map(|v| deserialize(v.1.as_ref()))
            .collect::<bincode::Result<_>>()?;

        Ok(list)
    }

    pub async fn update_event(&mut self, req: UpdateEventRequest) -> Result<Event> {
        let prev_event = self.get_event_by_id(req.project_id, req.id).await?;
        let idx_keys = index_keys(req.project_id, req.name.as_str(), &req.display_name);
        let idx_prev_keys = index_keys(prev_event.project_id, prev_event.name.as_str(), &prev_event.display_name);
        self.idx
            .check_update_constraints(
                idx_keys.as_ref(),
                idx_prev_keys.as_ref(),
            )
            .await?;

        let updated_at = Utc::now(); // TODO add updated_by
        let event = req.into_event(prev_event, updated_at, None);

        self.store
            .put(
                Key::Data(NAMESPACE, event.project_id, event.id).as_bytes(),
                serialize(&event)?,
            )
            .await?;

        self.idx
            .update(
                idx_keys.as_ref(),
                idx_prev_keys.as_ref(),
                event.id.to_le_bytes(),
            )
            .await?;
        Ok(event)
    }

    pub async fn delete_event(&mut self, project_id: u64, id: u64) -> Result<Event> {
        let event = self.get_event_by_id(project_id, id).await?;
        self.store
            .delete(Key::Data(NAMESPACE, project_id, id).as_bytes())
            .await?;

        self.idx.delete(index_keys(event.project_id, event.name.as_str(), &event.display_name).as_ref()).await?;
        Ok(event)
    }
}
