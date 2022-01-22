use crate::error::Error;
use crate::store::store::{make_data_key, make_id_seq_key, make_index_key, Store};
use crate::Result;

use crate::column::make_col_id_seq_key;
use crate::event_properties::types::{
    CreateEventPropertyRequest, EventProperty, IndexValues, Status, UpdateEventPropertyRequest,
};
use crate::store::index;
use bincode::{deserialize, serialize};
use chrono::Utc;
use std::sync::Arc;

const NAMESPACE: &[u8] = b"event_properties";
const IDX_NAME: &[u8] = b"event_name";
const IDX_DISPLAY_NAME: &[u8] = b"display_name";

fn index_keys(values: Box<&dyn IndexValues>) -> Vec<Option<Vec<u8>>> {
    if let Status::Disabled = values.status() {
        return vec![None, None];
    }
    if let Some(display_name) = values.display_name() {
        vec![
            Some(make_index_key(NAMESPACE, values.project_id(), IDX_NAME, values.name()).to_vec()),
            Some(
                make_index_key(
                    NAMESPACE,
                    values.project_id(),
                    IDX_DISPLAY_NAME,
                    display_name,
                )
                .to_vec(),
            ),
        ]
    } else {
        vec![
            Some(make_index_key(NAMESPACE, values.project_id(), IDX_NAME, values.name()).to_vec()),
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
        self.idx.check_insert_constraints(idx_keys.as_ref()).await?;

        let id = self
            .store
            .next_seq(make_id_seq_key(NAMESPACE, req.project_id))
            .await?;
        let created_at = Utc::now();
        let col_id = self
            .store
            .next_seq(make_col_id_seq_key(req.project_id))
            .await?;

        let prop = req.into_event_property(id, col_id, created_at);
        let data = serialize(&prop)?;
        self.store
            .put(make_data_key(NAMESPACE, prop.project_id, prop.id), &data)
            .await?;

        self.idx.insert(idx_keys.as_ref(), &data).await?;
        Ok(prop)
    }

    pub async fn get_by_id(&self, project_id: u64, id: u64) -> Result<EventProperty> {
        match self
            .store
            .get(make_data_key(NAMESPACE, project_id, id))
            .await?
        {
            None => Err(Error::EventPropertyDoesNotExist),
            Some(value) => Ok(deserialize(&value)?),
        }
    }

    pub async fn get_by_name(&self, project_id: u64, name: &str) -> Result<EventProperty> {
        let data = self
            .idx
            .get(make_index_key(NAMESPACE, project_id, IDX_NAME, name))
            .await?;

        Ok(deserialize(&data)?)
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
            .check_update_constraints(idx_keys.as_ref(), idx_prev_keys.as_ref())
            .await?;

        let updated_at = Utc::now(); // TODO add updated_by
        let prop = req.into_event_property(prev_prop, updated_at, None);
        let data = serialize(&prop)?;

        self.store
            .put(make_data_key(NAMESPACE, prop.project_id, prop.id), &data)
            .await?;

        self.idx
            .update(idx_keys.as_ref(), idx_prev_keys.as_ref(), &data)
            .await?;
        Ok(prop)
    }

    pub async fn delete(&mut self, project_id: u64, id: u64) -> Result<EventProperty> {
        let prop = self.get_by_id(project_id, id).await?;
        self.store
            .delete(make_data_key(NAMESPACE, project_id, id))
            .await?;

        self.idx
            .delete(index_keys(Box::new(&prop)).as_ref())
            .await?;
        Ok(prop)
    }
}
