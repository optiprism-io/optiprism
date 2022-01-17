use std::collections::HashMap;
use crate::store::store::{Namespace, Store};
use crate::Result;
use crate::error::Error;

use bincode::{deserialize, serialize};
use chrono::Utc;
use std::sync::Arc;
use types::event_property::{EventProperty, Scope};

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

pub struct Provider {
    store: Arc<Store>,
    name_idx: HashMap<NameKey, u64>,
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

    fn update_idx(&mut self, prop: &EventProperty, prev_prop: Option<&EventProperty>) {
        if let Some(p) = prev_prop {
            if p.name != prop.name {
                self.name_idx.remove(&name_key(&p));
            }
        }
        self.name_idx.insert(name_key(prop), prop.id);
    }

    pub async fn create_property(&mut self, prop: EventProperty) -> Result<EventProperty> {
        // name is unique among all properties
        match self.name_idx.get(&name_key(&prop)) {
            Some(_) => return Err(Error::EventPropertyWithSameNameAlreadyExist),
            None => {}
        }

        let mut prop = prop.clone();
        prop.created_at = Some(Utc::now());
        prop.id = self.store.next_seq(KV_NAMESPACE).await?;
        self.store
            .put(KV_NAMESPACE, prop.id.to_le_bytes(), serialize(&prop)?)
            .await?;
        self.update_idx(&prop, None);

        Ok(prop)
    }

    pub async fn update_property(&mut self, prop: EventProperty) -> Result<EventProperty> {
        let prev_prop = self.get_property_by_id(prop.id).await?;

        let mut prop = prop.clone();
        prop.updated_at = Some(Utc::now());

        self.store
            .put(KV_NAMESPACE, prop.id.to_le_bytes(), serialize(&prop)?)
            .await?;

        self.update_idx(&prop, Some(&prev_prop));
        Ok(prop)
    }

    pub async fn get_property_by_id(&self, id: u64) -> Result<EventProperty> {
        match self.store.get(KV_NAMESPACE, id.to_le_bytes()).await? {
            None => Err(Error::EventPropertyDoesNotExist),
            Some(value) => Ok(deserialize(&value)?),
        }
    }

    pub async fn get_property_by_name(&self, name: &str) -> Result<EventProperty> {
        match self.name_idx.get(name) {
            None => Err(Error::EventPropertyDoesNotExist),
            Some(id) => self.get_property_by_id(id.clone()).await
        }
    }

    pub async fn delete_property(&mut self, id: u64) -> Result<EventProperty> {
        let prop = self.get_property_by_id(id).await?;
        self.store.delete(KV_NAMESPACE, id.to_le_bytes()).await?;

        self.name_idx.remove(&prop.name);
        Ok(prop)
    }

    pub async fn list_event_properties(&self) -> Result<Vec<EventProperty>> {
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