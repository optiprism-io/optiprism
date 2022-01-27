use crate::error::Error;
use crate::store::store::{make_data_key, make_id_seq_key, make_index_key, Store};
use crate::Result;

use crate::events::types::{CreateEventRequest, UpdateEventRequest};
use crate::events::{Event, Status};
use crate::store::index;
use bincode::{deserialize, serialize};
use chrono::Utc;
use std::sync::{Arc, Mutex};
use crate::store::index::hash_map::HashMap;

const NAMESPACE: &[u8] = b"events";
const IDX_NAME: &[u8] = b"name";
const IDX_DISPLAY_NAME: &[u8] = b"display_name";

fn index_keys(organization_id: u64, project_id: u64, status: &Status, name: &str, display_name: &Option<String>) -> Vec<Option<Vec<u8>>> {
    if let Status::Disabled = status {
        return vec![None, None];
    }
    if let Some(display_name) = display_name {
        vec![
            Some(make_index_key(organization_id, project_id, NAMESPACE, IDX_NAME, name).to_vec()),
            Some(
                make_index_key(
                    organization_id,
                    project_id,
                    NAMESPACE,
                    IDX_DISPLAY_NAME,
                    display_name,
                )
                    .to_vec(),
            ),
        ]
    } else {
        vec![
            Some(make_index_key(organization_id, project_id, NAMESPACE, IDX_NAME, name).to_vec()),
            None,
        ]
    }
}

pub struct Provider {
    store: Arc<Store>,
    idx: Mutex<HashMap>,
}

impl Provider {
    pub fn new(kv: Arc<Store>) -> Self {
        Provider {
            store: kv.clone(),
            idx: Mutex::new(HashMap::new(kv)),
        }
    }

    pub async fn create(&self, organization_id: u64, req: CreateEventRequest) -> Result<Event> {
        let idx_keys = index_keys(organization_id, req.project_id, &req.status, &req.name, &req.display_name);
        let mut idx = self.idx.lock().map_err(|e| Error::Internal(e.to_string()))?;
        idx.check_insert_constraints(idx_keys.as_ref()).await?;

        let created_at = Utc::now();
        let id = self
            .store
            .next_seq(make_id_seq_key(organization_id, req.project_id, NAMESPACE))
            .await?;

        let event = Event {
            id,
            created_at,
            updated_at: None,
            created_by: req.created_by,
            updated_by: None,
            project_id: req.project_id,
            tags: req.tags,
            name: req.name,
            display_name: req.display_name,
            description: req.description,
            status: req.status,
            scope: req.scope,
            properties: req.properties,
            custom_properties: req.custom_properties,
        };
        let data = serialize(&event)?;
        self.store
            .put(
                make_data_key(organization_id, event.project_id, NAMESPACE, event.id),
                &data,
            )
            .await?;

        idx
            .insert(idx_keys.as_ref(), &data)
            .await?;
        Ok(event)
    }

    pub async fn get_by_id(&self, organization_id: u64, project_id: u64, id: u64) -> Result<Event> {
        match self
            .store
            .get(make_data_key(organization_id, project_id, NAMESPACE, id))
            .await?
        {
            None => Err(Error::KeyNotFound),
            Some(value) => Ok(deserialize(&value)?),
        }
    }

    pub async fn get_by_name(&self, organization_id: u64, project_id: u64, name: &str) -> Result<Event> {
        let idx = self.idx.lock().map_err(|e| Error::Internal(e.to_string()))?;

        let data = idx
            .get(make_index_key(organization_id, project_id, NAMESPACE, IDX_NAME, name))
            .await?;

        Ok(deserialize(&data)?)
    }

    pub async fn list(&self, organization_id: u64, project_id: u64) -> Result<Vec<Event>> {
        let list = self
            .store
            .list_prefix(b"/events/ent") // TODO doesn't work
            .await?
            .iter()
            .map(|v| deserialize(v.1.as_ref()))
            .collect::<bincode::Result<_>>()?;

        Ok(list)
    }

    pub async fn update(&self, organization_id: u64, req: UpdateEventRequest) -> Result<Event> {
        let idx_keys = index_keys(organization_id, req.project_id, &req.status, &req.name, &req.display_name);
        let prev_event = self.get_by_id(organization_id, req.project_id, req.id).await?;
        let idx_prev_keys = index_keys(organization_id, prev_event.project_id, &prev_event.status, &prev_event.name, &prev_event.display_name);
        let mut idx = self.idx.lock().map_err(|e| Error::Internal(e.to_string()))?;
        idx
            .check_update_constraints(idx_keys.as_ref(), idx_prev_keys.as_ref())
            .await?;

        let updated_at = Utc::now(); // TODO add updated_by
        let event = Event {
            id: req.id,
            created_at: prev_event.created_at,
            updated_at: Some(updated_at),
            created_by: prev_event.created_by,
            updated_by: Some(req.updated_by),
            project_id: req.project_id,
            tags: req.tags,
            name: req.name,
            display_name: req.display_name,
            description: req.description,
            status: req.status,
            scope: req.scope,
            properties: req.properties,
            custom_properties: req.custom_properties,
        };
        let data = serialize(&event)?;
        self.store
            .put(
                make_data_key(organization_id, event.project_id, NAMESPACE, event.id),
                &data,
            )
            .await?;

        idx
            .update(
                idx_keys.as_ref(),
                idx_prev_keys.as_ref(),
                &data,
            )
            .await?;
        Ok(event)
    }

    pub async fn attach_property(
        &self,
        organization_id: u64,
        project_id: u64,
        event_id: u64,
        prop_id: u64,
    ) -> Result<Event> {
        let mut event = self.get_by_id(organization_id, project_id, event_id).await?;
        event.properties = match event.properties {
            None => Some(vec![prop_id]),
            Some(props) => match props.iter().find(|x| prop_id == **x) {
                None => Some([props, vec![prop_id]].concat()),
                Some(_) => return Err(Error::ConstraintViolation),
            },
        };

        self.store
            .put(
                make_data_key(organization_id, event.project_id, NAMESPACE, event.id),
                serialize(&event)?,
            )
            .await?;
        Ok(event)
    }

    pub async fn detach_property(
        &self,
        organization_id: u64,
        project_id: u64,
        event_id: u64,
        prop_id: u64,
    ) -> Result<Event> {
        let mut event = self.get_by_id(organization_id, project_id, event_id).await?;
        event.properties = match event.properties {
            None => return Err(Error::ConstraintViolation),
            Some(props) => match props.iter().find(|x| prop_id == **x) {
                None => return Err(Error::ConstraintViolation),
                Some(_) => Some(props.into_iter().filter(|x| prop_id != *x).collect()),
            },
        };

        self.store
            .put(
                make_data_key(organization_id, event.project_id, NAMESPACE, event.id),
                serialize(&event)?,
            )
            .await?;
        Ok(event)
    }

    pub async fn delete(&self, organization_id: u64, project_id: u64, id: u64) -> Result<Event> {
        let event = self.get_by_id(organization_id, project_id, id).await?;
        self.store
            .delete(make_data_key(organization_id, project_id, NAMESPACE, id))
            .await?;

        let mut idx = self.idx.lock().map_err(|e| Error::Internal(e.to_string()))?;
        idx.delete(index_keys(organization_id, event.project_id, &event.status, &event.name, &event.display_name).as_ref())
            .await?;

        Ok(event)
    }
}
