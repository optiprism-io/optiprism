use std::sync::Arc;

use bincode::deserialize;
use bincode::serialize;
use chrono::DateTime;
use chrono::Utc;
use common::query::EventFilter;
use common::query::EventRef;
use common::types::OptionalProperty;
use rocksdb::Transaction;
use rocksdb::TransactionDB;
use serde::Deserialize;
use serde::Serialize;

use crate::error::MetadataError;
use crate::events::Events;
use crate::index::check_insert_constraints;
use crate::index::check_update_constraints;
use crate::index::delete_index;
use crate::index::get_index;
use crate::index::insert_index;
use crate::index::next_seq;
use crate::index::update_index;
use crate::list_data;
use crate::make_data_value_key;
use crate::make_id_seq_key;
use crate::make_index_key;
use crate::metadata::ListResponse;
use crate::project_ns;
use crate::Result;

const NAMESPACE: &[u8] = b"custom_events";
const IDX_NAME: &[u8] = b"name";
pub const MAX_EVENTS_LEVEL: usize = 3;

fn index_keys(project_id: u64, name: &str) -> Vec<Option<Vec<u8>>> {
    [index_name_key(project_id, name)].to_vec()
}

fn index_name_key(project_id: u64, name: &str) -> Option<Vec<u8>> {
    Some(make_index_key(project_ns(project_id, NAMESPACE).as_slice(), IDX_NAME, name).to_vec())
}

pub struct CustomEvents {
    db: Arc<TransactionDB>,
    events: Arc<Events>,
    max_events_level: usize,
}

impl CustomEvents {
    pub fn new(db: Arc<TransactionDB>, events: Arc<Events>) -> Self {
        CustomEvents {
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
                    self.events.get_by_name(project_id, name.as_str())?;
                }
                EventRef::Regular(id) => {
                    self.events.get_by_id(project_id, *id)?;
                }
                EventRef::Custom(id) => {
                    if ids.contains(id) {
                        return Err(MetadataError::AlreadyExists(
                            "custom event already exist".to_string(),
                        ));
                    }
                    let custom_event = self.get_by_id(project_id, *id)?;
                    ids.push(custom_event.id);
                    self.validate_events(project_id, &custom_event.events, level + 1, ids)?;
                }
            }
        }

        Ok(())
    }

    fn get_by_id_(
        &self,
        tx: &Transaction<TransactionDB>,
        project_id: u64,
        id: u64,
    ) -> Result<CustomEvent> {
        let key = make_data_value_key(project_ns(project_id, NAMESPACE).as_slice(), id);

        match tx.get(key)? {
            None => Err(MetadataError::NotFound(
                format!("custom event {id} not found").to_string(),
            )),
            Some(value) => Ok(deserialize(&value)?),
        }
    }

    pub fn create(&self, project_id: u64, req: CreateCustomEventRequest) -> Result<CustomEvent> {
        if req.events.is_empty() {
            return Err(MetadataError::BadRequest("empty events".to_string()));
        }

        let mut ids = Vec::new();
        self.validate_events(project_id, &req.events, 0, &mut ids)?;

        let idx_keys = index_keys(project_id, &req.name);

        let tx = self.db.transaction();
        check_insert_constraints(&tx, idx_keys.as_ref())?;

        let created_at = Utc::now();
        let id = next_seq(
            &tx,
            make_id_seq_key(project_ns(project_id, NAMESPACE).as_slice()),
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
            make_data_value_key(project_ns(project_id, NAMESPACE).as_slice(), event.id),
            &data,
        )?;

        insert_index(&tx, idx_keys.as_ref(), event.id)?;
        tx.commit()?;
        Ok(event)
    }

    pub fn get_by_id(&self, project_id: u64, id: u64) -> Result<CustomEvent> {
        let tx = self.db.transaction();

        self.get_by_id_(&tx, project_id, id)
    }

    pub fn get_by_name(&self, project_id: u64, name: &str) -> Result<CustomEvent> {
        let tx = self.db.transaction();
        let id = get_index(
            &tx,
            make_index_key(project_ns(project_id, NAMESPACE).as_slice(), IDX_NAME, name),
            format!("custom event with name \"{}\" not found", name).as_str(),
        )?;
        self.get_by_id_(&tx, project_id, id)
    }

    pub fn list(&self, project_id: u64) -> Result<ListResponse<CustomEvent>> {
        let tx = self.db.transaction();
        list_data(&tx, project_ns(project_id, NAMESPACE).as_slice())
    }

    pub fn update(
        &self,
        project_id: u64,
        event_id: u64,
        req: UpdateCustomEventRequest,
    ) -> Result<CustomEvent> {
        let tx = self.db.transaction();

        let prev_event = self.get_by_id_(&tx, project_id, event_id)?;
        let mut event = prev_event.clone();

        let mut idx_keys: Vec<Option<Vec<u8>>> = Vec::new();
        let mut idx_prev_keys: Vec<Option<Vec<u8>>> = Vec::new();
        if let OptionalProperty::Some(name) = &req.name {
            idx_keys.push(index_name_key(project_id, name.as_str()));
            idx_prev_keys.push(index_name_key(project_id, prev_event.name.as_str()));
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
            self.validate_events(project_id, &events, 0, &mut ids)?;

            event.events = events;
        }

        let data = serialize(&event)?;
        tx.put(
            make_data_value_key(project_ns(project_id, NAMESPACE).as_slice(), event.id),
            &data,
        )?;

        update_index(&tx, idx_keys.as_ref(), idx_prev_keys.as_ref(), event_id)?;
        tx.commit()?;
        Ok(event)
    }

    pub fn delete(&self, project_id: u64, id: u64) -> Result<CustomEvent> {
        let tx = self.db.transaction();
        let event = self.get_by_id_(&tx, project_id, id)?;
        tx.delete(make_data_value_key(
            project_ns(project_id, NAMESPACE).as_slice(),
            id,
        ))?;

        delete_index(&tx, index_keys(project_id, &event.name).as_ref())?;
        tx.commit()?;
        Ok(event)
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub enum Status {
    #[default]
    Enabled,
    Disabled,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Event {
    pub event: EventRef,
    pub filters: Option<Vec<EventFilter>>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct CustomEvent {
    pub id: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
    pub created_by: u64,
    pub updated_by: Option<u64>,
    pub project_id: u64,
    pub tags: Option<Vec<String>>,
    pub name: String,
    pub description: Option<String>,
    pub status: Status,
    pub is_system: bool,
    pub events: Vec<Event>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct CreateCustomEventRequest {
    pub created_by: u64,
    pub tags: Option<Vec<String>>,
    pub name: String,
    pub description: Option<String>,
    pub status: Status,
    pub is_system: bool,
    pub events: Vec<Event>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct UpdateCustomEventRequest {
    pub updated_by: u64,
    pub tags: OptionalProperty<Option<Vec<String>>>,
    pub name: OptionalProperty<String>,
    pub description: OptionalProperty<Option<String>>,
    pub status: OptionalProperty<Status>,
    pub is_system: OptionalProperty<bool>,
    pub events: OptionalProperty<Vec<Event>>,
}
