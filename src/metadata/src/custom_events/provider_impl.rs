use std::sync::Arc;
use std::sync::RwLock;

use async_trait::async_trait;
use bincode::deserialize;
use bincode::serialize;
use chrono::Utc;
use common::query::EventRef;
use common::types::OptionalProperty;
use futures::future::BoxFuture;
use futures::future::FutureExt;
use rocksdb::Transaction;
use rocksdb::TransactionDB;

use crate::custom_events::CreateCustomEventRequest;
use crate::custom_events::CustomEvent;
use crate::custom_events::Event;
use crate::custom_events::Provider;
use crate::custom_events::UpdateCustomEventRequest;
use crate::error;
use crate::error::MetadataError;
use crate::events;
use crate::index::check_insert_constraints;
use crate::index::check_update_constraints;
use crate::index::delete_index;
use crate::index::get_index;
use crate::index::insert_index;
use crate::index::next_seq;
use crate::index::update_index;
use crate::metadata::ListResponse;
use crate::store::path_helpers::list;
use crate::store::path_helpers::make_data_value_key;
use crate::store::path_helpers::make_id_seq_key;
use crate::store::path_helpers::make_index_key;
use crate::store::path_helpers::org_proj_ns;
use crate::store::Store;
use crate::Result;

const NAMESPACE: &[u8] = b"custom_events";
const IDX_NAME: &[u8] = b"name";
pub const MAX_EVENTS_LEVEL: usize = 3;

fn index_keys(organization_id: u64, project_id: u64, name: &str) -> Vec<Option<Vec<u8>>> {
    [index_name_key(organization_id, project_id, name)].to_vec()
}

fn index_name_key(organization_id: u64, project_id: u64, name: &str) -> Option<Vec<u8>> {
    Some(
        make_index_key(
            org_proj_ns(organization_id, project_id, NAMESPACE).as_slice(),
            IDX_NAME,
            name,
        )
        .to_vec(),
    )
}

pub struct ProviderImpl {
    db: Arc<TransactionDB>,
    events: Arc<dyn events::Provider>,
    max_events_level: usize,
}

impl ProviderImpl {
    pub fn new(db: Arc<TransactionDB>, events: Arc<dyn events::Provider>) -> Self {
        ProviderImpl {
            db,
            events,
            max_events_level: MAX_EVENTS_LEVEL,
        }
    }

    pub fn with_max_events_level(self, max_events_level: usize) -> Self {
        Self {
            db: self.db,
            events: self.events,
            max_events_level,
        }
    }

    fn validate_events<'a>(
        &'a self,
        organization_id: u64,
        project_id: u64,
        events: &'a [Event],
        level: usize,
        ids: &'a mut Vec<u64>,
    ) -> Result<()> {
        if level > self.max_events_level {
            return Err(MetadataError::BadRequest(format!(
                "max level exceeded: {}",
                self.max_events_level
            )));
        }

        for event in events.iter() {
            match &event.event {
                EventRef::RegularName(name) => {
                    self.events
                        .get_by_name(organization_id, project_id, name.as_str())?;
                }
                EventRef::Regular(id) => {
                    self.events.get_by_id(organization_id, project_id, *id)?;
                }
                EventRef::Custom(id) => {
                    if ids.contains(id) {
                        return Err(MetadataError::AlreadyExists(
                            "custom event already exist".to_string(),
                        ));
                    }
                    let custom_event = self.get_by_id(organization_id, project_id, *id)?;
                    ids.push(custom_event.id);
                    self.validate_events(
                        organization_id,
                        project_id,
                        &custom_event.events,
                        level + 1,
                        ids,
                    )?;
                }
            }
        }

        Ok(())
    }

    fn _get_by_id(
        &self,
        tx: &Transaction<TransactionDB>,
        organization_id: u64,
        project_id: u64,
        id: u64,
    ) -> Result<CustomEvent> {
        let key = make_data_value_key(
            org_proj_ns(organization_id, project_id, NAMESPACE).as_slice(),
            id,
        );

        match tx.get(key)? {
            None => Err(MetadataError::NotFound(
                "custom event not found".to_string(),
            )),
            Some(value) => Ok(deserialize(&value)?),
        }
    }
}

impl Provider for ProviderImpl {
    fn create(
        &self,
        organization_id: u64,
        project_id: u64,
        req: CreateCustomEventRequest,
    ) -> Result<CustomEvent> {
        if req.events.is_empty() {
            return Err(MetadataError::BadRequest("empty events".to_string()));
        }

        let mut ids = Vec::new();
        self.validate_events(organization_id, project_id, &req.events, 0, &mut ids)?;

        let idx_keys = index_keys(organization_id, project_id, &req.name);

        let tx = self.db.transaction();
        check_insert_constraints(&tx, idx_keys.as_ref())?;

        let created_at = Utc::now();
        let id = next_seq(
            &tx,
            make_id_seq_key(org_proj_ns(organization_id, project_id, NAMESPACE).as_slice()),
        )?;

        let event = CustomEvent {
            id,
            created_at,
            updated_at: None,
            created_by: req.created_by,
            updated_by: None,
            project_id,
            tags: req.tags,
            name: req.name,
            description: req.description,
            status: req.status,
            is_system: req.is_system,
            events: req.events,
        };
        let data = serialize(&event)?;
        self.db.put(
            make_data_value_key(
                org_proj_ns(organization_id, project_id, NAMESPACE).as_slice(),
                event.id,
            ),
            &data,
        )?;

        insert_index(&tx, idx_keys.as_ref(), &data)?;
        tx.commit()?;
        Ok(event)
    }

    fn get_by_id(&self, organization_id: u64, project_id: u64, id: u64) -> Result<CustomEvent> {
        let tx = self.db.transaction();

        self._get_by_id(&tx, organization_id, project_id, id)
    }

    fn get_by_name(
        &self,
        organization_id: u64,
        project_id: u64,
        name: &str,
    ) -> Result<CustomEvent> {
        let tx = self.db.transaction();
        let data = get_index(
            &tx,
            make_index_key(
                org_proj_ns(organization_id, project_id, NAMESPACE).as_slice(),
                IDX_NAME,
                name,
            ),
        )?;
        Ok(deserialize::<CustomEvent>(&data)?)
    }

    fn list(&self, organization_id: u64, project_id: u64) -> Result<ListResponse<CustomEvent>> {
        let tx = self.db.transaction();
        list(
            &tx,
            org_proj_ns(organization_id, project_id, NAMESPACE).as_slice(),
        )
    }

    fn update(
        &self,
        organization_id: u64,
        project_id: u64,
        event_id: u64,
        req: UpdateCustomEventRequest,
    ) -> Result<CustomEvent> {
        let tx = self.db.transaction();

        let prev_event = self._get_by_id(&tx, organization_id, project_id, event_id)?;
        let mut event = prev_event.clone();

        let mut idx_keys: Vec<Option<Vec<u8>>> = Vec::new();
        let mut idx_prev_keys: Vec<Option<Vec<u8>>> = Vec::new();
        if let OptionalProperty::Some(name) = &req.name {
            idx_keys.push(index_name_key(organization_id, project_id, name.as_str()));
            idx_prev_keys.push(index_name_key(
                organization_id,
                project_id,
                prev_event.name.as_str(),
            ));
            event.name = name.to_owned();
        }

        check_update_constraints(&tx, idx_keys.as_ref(), idx_prev_keys.as_ref())?;

        event.updated_at = Some(Utc::now());
        event.updated_by = Some(req.updated_by);
        if let OptionalProperty::Some(tags) = req.tags {
            event.tags = tags;
        }
        if let OptionalProperty::Some(description) = req.description {
            event.description = description;
        }
        if let OptionalProperty::Some(status) = req.status {
            event.status = status;
        }
        if let OptionalProperty::Some(is_system) = req.is_system {
            event.is_system = is_system;
        }

        if let OptionalProperty::Some(events) = req.events {
            if events.is_empty() {
                return Err(MetadataError::BadRequest("empty events".to_string()));
            }

            let mut ids = vec![event.id];
            self.validate_events(organization_id, project_id, &events, 0, &mut ids)?;

            event.events = events;
        }

        let data = serialize(&event)?;
        tx.put(
            make_data_value_key(
                org_proj_ns(organization_id, project_id, NAMESPACE).as_slice(),
                event.id,
            ),
            &data,
        )?;

        update_index(&tx, idx_keys.as_ref(), idx_prev_keys.as_ref(), &data)?;
        tx.commit()?;
        Ok(event)
    }

    fn delete(&self, organization_id: u64, project_id: u64, id: u64) -> Result<CustomEvent> {
        let tx = self.db.transaction();
        let event = self._get_by_id(&tx, organization_id, project_id, id)?;
        tx.delete(make_data_value_key(
            org_proj_ns(organization_id, project_id, NAMESPACE).as_slice(),
            id,
        ))?;

        delete_index(
            &tx,
            index_keys(organization_id, project_id, &event.name).as_ref(),
        )?;
        tx.commit()?;
        Ok(event)
    }
}
