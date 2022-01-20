use crate::error::Error;
use crate::store::store::Store;
use crate::Result;

use crate::store::index;
use bincode::{deserialize, serialize};
use chrono::Utc;
use std::sync::Arc;
use types::event::Event;

#[derive(Clone)]
enum Key {
    // /events/data/{project_id}/{event_id}
    Data(u64, u64),
    // /events/idx/{project_id}/{idx_name}/{idx_value}
    Index(u64, String, Vec<u8>),
    // /events/id_seq
    IdSequence,
}

impl Key {
    fn as_bytes(&self) -> Vec<u8> {
        match self {
            Key::Data(project_id, event_id) => [
                b"/events/data/",
                project_id.to_le_bytes().as_ref(),
                b"/",
                event_id.to_le_bytes().as_ref(),
            ]
            .concat(),
            Key::Index(project_id, idx_name, key) => [
                b"/events/idx/",
                project_id.to_le_bytes().as_ref(),
                b"/",
                idx_name.as_bytes(),
                b"/",
                key,
            ]
            .concat(),
            Key::IdSequence => "/events/id_seq/".as_bytes().to_vec(),
        }
    }
}

fn indexes(event: &Event) -> Vec<Option<(Vec<u8>, Vec<u8>)>> {
    vec![Some((
        [
            b"/events/idx/",
            event.project_id.to_le_bytes().as_slice(),
            b"/name/",
            event.name.as_bytes(),
        ]
        .concat(),
        event.id.to_le_bytes().to_vec(),
    ))]
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

    pub async fn create_event(&mut self, event: Event) -> Result<Event> {
        self.idx
            .check_insert_constraints(indexes(&event).as_ref())
            .await?;

        let mut event = event.clone();
        event.created_at = Some(Utc::now());
        event.id = self.store.next_seq(Key::IdSequence.as_bytes()).await?;
        self.store
            .put(
                Key::Data(event.project_id, event.id).as_bytes(),
                serialize(&event)?,
            )
            .await?;

        self.idx.insert(indexes(&event).as_ref()).await?;
        Ok(event)
    }

    pub async fn update_event(&mut self, event: Event) -> Result<Event> {
        let prev_event = self.get_event_by_id(event.project_id, event.id).await?;
        self.idx
            .check_update_constraints(indexes(&event).as_ref(), indexes(&prev_event).as_ref())
            .await?;

        let mut event = event.clone();
        event.updated_at = Some(Utc::now());

        self.store
            .put(
                Key::Data(event.project_id, event.id).as_bytes(),
                serialize(&event)?,
            )
            .await?;

        self.idx
            .update(indexes(&event).as_ref(), indexes(&prev_event).as_ref())
            .await?;
        Ok(event)
    }

    pub async fn get_event_by_id(&self, project_id: u64, id: u64) -> Result<Event> {
        match self.store.get(Key::Data(project_id, id).as_bytes()).await? {
            None => Err(Error::EventDoesNotExist),
            Some(value) => Ok(deserialize(&value)?),
        }
    }

    pub async fn get_event_by_name(&self, project_id: u64, name: &str) -> Result<Event> {
        let id = self
            .idx
            .get(Key::Index(project_id, "name".to_string(), name.as_bytes().to_vec()).as_bytes())
            .await?;
        self.get_event_by_id(project_id, u64::from_le_bytes(id.try_into()?))
            .await
    }

    pub async fn delete_event(&mut self, project_id: u64, id: u64) -> Result<Event> {
        let event = self.get_event_by_id(project_id, id).await?;
        self.store
            .delete(Key::Data(project_id, id).as_bytes())
            .await?;

        self.idx.delete(indexes(&event).as_ref()).await?;
        Ok(event)
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
}
