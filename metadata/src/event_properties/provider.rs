use crate::store::store::{make_data_key, make_id_seq_key, make_index_key, Store};
use crate::Result;
use crate::error::Error;

use bincode::{deserialize, serialize};
use chrono::Utc;
use std::sync::Arc;
use crate::column::make_col_id_seq_key;
use crate::event_properties::types::{CreateEventPropertyRequest, EventProperty, IndexValues, Scope, Status, UpdateEventPropertyRequest};
use crate::store::index;

const NAMESPACE: &[u8] = b"event_properties";
const IDX_EVENT_NAME: &[u8] = b"event_name";
const IDX_GLOBAL_NAME: &[u8] = b"global_name";
const IDX_DISPLAY_NAME: &[u8] = b"display_name";

fn make_name_index_key(project_id: u64, scope: Scope, key: &str) -> Vec<u8> {
    match scope {
        // event_properties/idx/{project_id}/event_name/{event_id}/{name}
        Scope::Event(event_id) => [NAMESPACE, b"/idx/", project_id.to_le_bytes().as_ref(), b"/", IDX_EVENT_NAME, b"/", event_id.to_le_bytes().as_ref(), b"/", key.as_bytes()].concat(),
        // event_properties/idx/{project_id}/name/{name}
        Scope::Global => [NAMESPACE, b"/idx/", project_id.to_le_bytes().as_ref(), b"/", IDX_GLOBAL_NAME, b"/", key.as_bytes()].concat()
    }
}

fn index_keys(values: Box<&dyn IndexValues>) -> Vec<Option<Vec<u8>>> {
    if let Status::Disabled = values.status() {
        return vec![None, None];
    }

    if let Some(display_name) = values.display_name() {
        vec![
            Some(make_name_index_key(values.project_id(), values.scope(), values.name()).to_vec()),
            Some(make_index_key(NAMESPACE, values.project_id(), IDX_DISPLAY_NAME, display_name).to_vec()),
        ]
    } else {
        vec![
            Some(make_name_index_key(values.project_id(), values.scope(), values.name()).to_vec()),
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

    pub async fn create(&mut self, req: CreateEventPropertyRequest) -> Result<EventProperty> {
        let idx_keys = index_keys(Box::new(&req));
        self.idx
            .check_insert_constraints(idx_keys.as_ref())
            .await?;

        let id = self.store.next_seq(make_id_seq_key(NAMESPACE, req.project_id)).await?;
        let created_at = Utc::now();
        let col_id = self.store.next_seq(make_col_id_seq_key(req.project_id)).await?;

        let prop = req.into_event_property(id, col_id, created_at);
        self.store
            .put(
                make_data_key(NAMESPACE, prop.project_id, prop.id),
                serialize(&prop)?,
            )
            .await?;

        self.idx.insert(idx_keys.as_ref(), prop.id.to_le_bytes()).await?;
        Ok(prop)
    }

    pub async fn get_by_id(&self, project_id: u64, id: u64) -> Result<EventProperty> {
        match self.store.get(make_data_key(NAMESPACE, project_id, id)).await? {
            None => Err(Error::EventPropertyDoesNotExist),
            Some(value) => Ok(deserialize(&value)?),
        }
    }

    pub async fn get_by_name(&self, project_id: u64, scope: Scope, name: &str) -> Result<EventProperty> {
        let id = self
            .idx
            .get(make_name_index_key(project_id, scope, name))
            .await?;
        self.get_by_id(project_id, u64::from_le_bytes(id.try_into()?))
            .await
    }

    pub async fn list(&self) -> Result<Vec<EventProperty>> {
        let list = self
            .store
            .list_prefix(b"/event_properties/ent") // TODO doesn't work
            .await?
            .iter()
            .map(|v| deserialize(v.1.as_ref()))
            .collect::<bincode::Result<_>>()?;

        Ok(list)
    }

    pub async fn update(&mut self, req: UpdateEventPropertyRequest) -> Result<EventProperty> {
        let idx_keys = index_keys(Box::new(&req));
        let prev_prop = self.get_by_id(req.project_id, req.id).await?;
        let idx_prev_keys = index_keys(Box::new(&prev_prop));
        self.idx
            .check_update_constraints(
                idx_keys.as_ref(),
                idx_prev_keys.as_ref(),
            )
            .await?;

        let updated_at = Utc::now(); // TODO add updated_by
        let prop = req.into_event_property(prev_prop, updated_at, None);

        self.store
            .put(
                make_data_key(NAMESPACE, prop.project_id, prop.id),
                serialize(&prop)?,
            )
            .await?;

        self.idx
            .update(
                idx_keys.as_ref(),
                idx_prev_keys.as_ref(),
                prop.id.to_le_bytes(),
            )
            .await?;
        Ok(prop)
    }

    pub async fn delete(&mut self, project_id: u64, id: u64) -> Result<EventProperty> {
        let prop = self.get_by_id(project_id, id).await?;
        self.store
            .delete(make_data_key(NAMESPACE, project_id, id))
            .await?;

        self.idx.delete(index_keys(Box::new(&prop)).as_ref()).await?;
        Ok(prop)
    }
}
