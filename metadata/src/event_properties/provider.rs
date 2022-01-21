use std::collections::HashMap;
use crate::store::store::{Namespace, Store};
use crate::Result;
use crate::error::Error;

use bincode::{deserialize, serialize};
use chrono::Utc;
use std::sync::Arc;
use types::event_property::{Column, EventProperty, Scope};

const KV_NAMESPACE: Namespace = Namespace::EventProperties;

#[derive(Eq, PartialEq, Hash)]
enum NameKey {
    Event(u64, u64, String),
    Global(u64, String),
}

fn name_key(prop: &EventProperty) -> NameKey {
    match prop.scope {
        Scope::Event(event_id) => NameKey::Event(prop.project_id, event_id, prop.name.clone()),
        Scope::Global => NameKey::Global(prop.project_id, prop.name.clone())
    }
}

struct sIndex {
    unique: bool,

}

struct Indexes {}

type DisplayNameKey = (u64, String);

fn display_name_key(prop: &EventProperty) -> Option<DisplayNameKey> {
    match prop.display_name.clone() {
        None => None,
        Some(name) => Some((prop.project_id, name.clone()))
    }
}


enum IndexOp<'a> {
    Insert(&'a EventProperty),
    Update {
        prop: &'a EventProperty,
        prev_prop: &'a EventProperty,
    },
    Delete(&'a EventProperty),
}

pub struct Provider {
    store: Arc<Store>,
    name_idx: HashMap<NameKey, u64>,
    display_name_idx: HashMap<DisplayNameKey, u64>,
}

impl Provider {
    pub fn try_new(kv: Arc<Store>) -> Result<Self> {
        let prov = Provider {
            store: kv.clone(),
            name_idx: HashMap::new(),
            display_name_idx: HashMap::new(),
        };
        prov.init()?;
        Ok(prov)
    }

    fn init(&self) -> Result<()> {
        Ok(())
    }

    fn update_indices(&mut self, op: IndexOp) {
        // todo: state machines library?
        match op {
            IndexOp::Insert(prop) => {
                self.name_idx.insert(name_key(prop), prop.id);
                match display_name_key(prop) {
                    None => {}
                    Some(key) => { self.display_name_idx.insert(key, prop.id); }
                }
            }

            IndexOp::Update { prop, prev_prop } => {
                if prop.name != prev_prop.name {
                    self.name_idx.remove(&name_key(prev_prop));
                    self.name_idx.insert(name_key(prop), prop.id);
                }
                self.name_idx.insert(name_key(prop), prop.id);

                match display_name_key(prev_prop) {
                    None => {}
                    Some(key) => { self.display_name_idx.remove(&key); }
                }

                match display_name_key(prop) {
                    None => {}
                    Some(key) => { self.display_name_idx.insert(key, prop.id); }
                }
            }
            IndexOp::Delete(prop) => {
                self.name_idx.remove(&name_key(prop));
                match display_name_key(prop) {
                    None => {}
                    Some(key) => { self.display_name_idx.remove(&key); }
                }
            }
        }
    }

    fn check_constraints(&mut self, op: IndexOp) -> Result<()> {
        match op {
            IndexOp::Insert(prop) => {
                if let Some(_) = self.name_idx.get(&name_key(prop)) {
                    return Err(Error::EventPropertyWithSameDisplayNameAlreadyExist);
                }
            }
            IndexOp::Update { prop, prev_prop } => {
                if prop.name != prev_prop.name {
                    if let Some(_) = self.name_idx.get(&name_key(prop)) {
                        return Err(Error::EventPropertyWithSameDisplayNameAlreadyExist);
                    }
                }

                match (display_name_key(prop), display_name_key(prev_prop)) {
                    (Some(key), None) => {
                        if let Some(_) = self.display_name_idx.get(&key) {
                            return Err(Error::EventPropertyWithSameDisplayNameAlreadyExist);
                        }
                    }
                    (Some(key), Some(prev_key)) => {
                        if key != prev_key {
                            if let Some(_) = self.display_name_idx.get(&key) {
                                return Err(Error::EventPropertyWithSameDisplayNameAlreadyExist);
                            }
                        }
                    }

                    None => {}
                    Some(key) => { self.display_name_idx.remove(&key); }
                }
            }
            _ => unreachable!(),
        }

        Ok(())
    }

    pub async fn create_event_property(&mut self, prop: EventProperty) -> Result<EventProperty> {
        self.check_constraints(IndexOp::Insert(&prop))?;

        let mut prop = prop.clone();
        prop.created_at = Some(Utc::now());
        prop.id = self.store.next_seq(KV_NAMESPACE).await?;
        match &prop.db_col {
            Column::Index(_) => return Err(Error::EventPropertyColumnShouldBeEmpty),
            Column::None => {
                let col_id = self.store.next_seq(Namespace::PropertyColumnSequences).await?;
                prop.db_col = Column::Index(col_id as usize);
            }
            Column::Name(name) => {}
        }
        self.store
            .put(KV_NAMESPACE, prop.id.to_le_bytes(), serialize(&prop)?)
            .await?;

        self.update_indices(IndexOp::Insert(&prop));
        Ok(prop)
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
