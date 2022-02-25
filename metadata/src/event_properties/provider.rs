use crate::error::Error;
use crate::store::store::{make_col_id_seq_key, make_data_value_key, make_id_seq_key, make_index_key, Store};
use crate::Result;

use crate::event_properties::types::{
    CreateEventPropertyRequest, EventProperty, Status, UpdateEventPropertyRequest,
};
use bincode::{deserialize, serialize};
use chrono::Utc;
use std::sync::{Arc, Mutex};
use tokio::sync::RwLock;
use crate::store::index::hash_map::HashMap;

const NAMESPACE: &[u8] = b"event_properties";
const IDX_NAME: &[u8] = b"event_name";
const IDX_DISPLAY_NAME: &[u8] = b"display_name";

fn index_keys(organization_id: u64, project_id: u64, name: &str, display_name: &Option<String>) -> Vec<Option<Vec<u8>>> {
    let mut idx: Vec<Option<Vec<u8>>> = vec![];
    idx.push(Some(make_index_key(organization_id, project_id, NAMESPACE, IDX_NAME, name).to_vec()));
    idx.push(
        display_name.as_ref().map(|display_name| {
            make_index_key(
                organization_id,
                project_id,
                NAMESPACE,
                IDX_DISPLAY_NAME,
                display_name,
            )
                .to_vec()
        })
    );

    idx
}

pub struct Provider {
    store: Arc<Store>,
    idx: HashMap,
    guard: RwLock<()>,
}

impl Provider {
    pub fn new(kv: Arc<Store>) -> Self {
        Provider {
            store: kv.clone(),
            idx: HashMap::new(kv),
            guard: RwLock::new(()),
        }
    }

    pub async fn create(&self, organization_id: u64, req: CreateEventPropertyRequest) -> Result<EventProperty> {
        let _guard = self.guard.write().await;
        let idx_keys = index_keys(organization_id, req.project_id, &req.name, &req.display_name);
        self.idx.check_insert_constraints(idx_keys.as_ref()).await?;

        let id = self
            .store
            .next_seq(make_id_seq_key(organization_id, req.project_id, NAMESPACE))
            .await?;
        let created_at = Utc::now();
        let col_id = self
            .store
            .next_seq(make_col_id_seq_key(organization_id, req.project_id))
            .await?;

        let prop = EventProperty {
            id,
            created_at,
            updated_at: None,
            created_by: req.created_by,
            updated_by: None,
            project_id: req.project_id,
            tags: req.tags,
            name: req.name,
            description: req.description,
            display_name: req.display_name,
            typ: req.typ,
            col_id,
            status: req.status,
            scope: req.scope,
            nullable: req.nullable,
            is_array: req.is_array,
            is_dictionary: req.is_dictionary,
            dictionary_type: req.dictionary_type,
        };

        let data = serialize(&prop)?;
        self.store
            .put(make_data_value_key(organization_id, prop.project_id, NAMESPACE, prop.id), &data).await?;

        self.idx.insert(idx_keys.as_ref(), &data).await?;
        Ok(prop)
    }

    pub async fn get_by_id(&self, organization_id: u64, project_id: u64, id: u64) -> Result<EventProperty> {
        let _guard = self.guard.read().await;
        match self
            .store
            .get(make_data_value_key(organization_id, project_id, NAMESPACE, id))
            .await?
        {
            None => Err(Error::KeyNotFound),
            Some(value) => Ok(deserialize(&value)?),
        }
    }

    pub async fn get_by_name(&self, organization_id: u64, project_id: u64, name: &str) -> Result<EventProperty> {
        let _guard = self.guard.read().await;
        let data = self.idx
            .get(make_index_key(organization_id, project_id, NAMESPACE, IDX_NAME, name))
            .await?;

        Ok(deserialize(&data)?)
    }

    pub async fn list(&self, _: u64, _: u64) -> Result<Vec<EventProperty>> {
        let list = self
            .store
            .list_prefix(b"/event_properties/ent") // TODO doesn't work
            .await?
            .iter()
            .map(|v| deserialize(v.1.as_ref()))
            .collect::<bincode::Result<_>>()?;

        Ok(list)
    }

    pub async fn update(&self, organization_id: u64, req: UpdateEventPropertyRequest) -> Result<EventProperty> {
        let _guard = self.guard.write().await;
        let idx_keys = index_keys(organization_id, req.project_id, &req.name, &req.display_name);
        let prev_prop = self.get_by_id(organization_id, req.project_id, req.id).await?;
        let idx_prev_keys = index_keys(organization_id, prev_prop.project_id, &prev_prop.name, &prev_prop.display_name);
        self.idx
            .check_update_constraints(idx_keys.as_ref(), idx_prev_keys.as_ref())
            .await?;

        let updated_at = Utc::now();
        let prop = EventProperty {
            id: req.id,
            created_at: prev_prop.created_at,
            updated_at: Some(updated_at),
            created_by: req.created_by,
            updated_by: req.updated_by,
            project_id: req.project_id,
            tags: req.tags,
            name: req.name,
            description: req.description,
            display_name: req.display_name,
            typ: req.typ,
            col_id: prev_prop.col_id,
            status: req.status,
            scope: req.scope,
            nullable: req.nullable,
            is_array: req.is_array,
            is_dictionary: req.is_dictionary,
            dictionary_type: req.dictionary_type,
        };
        let data = serialize(&prop)?;

        self.store
            .put(make_data_value_key(organization_id, prop.project_id, NAMESPACE, prop.id), &data)
            .await?;

        self.idx
            .update(idx_keys.as_ref(), idx_prev_keys.as_ref(), &data)
            .await?;
        Ok(prop)
    }

    pub async fn delete(&self, organization_id: u64, project_id: u64, id: u64) -> Result<EventProperty> {
        let _guard = self.guard.write().await;
        let prop = self.get_by_id(organization_id, project_id, id).await?;
        self.store
            .delete(make_data_value_key(organization_id, project_id, NAMESPACE, id))
            .await?;

        self.idx
            .delete(index_keys(organization_id, prop.project_id, &prop.name, &prop.display_name).as_ref())
            .await?;
        Ok(prop)
    }
}
