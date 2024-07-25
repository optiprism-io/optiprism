use std::str::from_utf8;
use std::str::pattern::Pattern;
use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use prost::Message;
use common::types::{OptionalProperty, TABLE_EVENTS};
use common::types::COLUMN_EVENT;
use rocksdb::Transaction;
use rocksdb::TransactionDB;
use serde::Deserialize;
use serde::Serialize;
use crate::dashboards::{Dashboard, Panel, Type};
use crate::dictionaries::Dictionaries;
use crate::error::MetadataError;
use crate::index::check_insert_constraints;
use crate::index::check_update_constraints;
use crate::index::delete_index;
use crate::index::get_index;
use crate::index::insert_index;
use crate::index::next_seq;
use crate::index::update_index;
use crate::{dashboard, event, list_data, make_data_key};
use crate::make_data_value_key;
use crate::make_id_seq_key;
use crate::make_index_key;
use crate::metadata::{ListResponse, ResponseMetadata};
use crate::project_ns;
use crate::Result;

const NAMESPACE: &[u8] = b"events";
const RECORDS_NAMESPACE: &[u8] = b"events/records";
const IDX_NAME: &[u8] = b"name";
const IDX_DISPLAY_NAME: &[u8] = b"display_name";

fn index_keys(project_id: u64, name: &str, display_name: Option<String>) -> Vec<Option<Vec<u8>>> {
    [
        index_name_key(project_id, name),
        index_display_name_key(project_id, display_name),
    ]
        .to_vec()
}

fn index_name_key(project_id: u64, name: &str) -> Option<Vec<u8>> {
    Some(make_index_key(project_ns(project_id, NAMESPACE).as_slice(), IDX_NAME, name).to_vec())
}

fn index_display_name_key(project_id: u64, display_name: Option<String>) -> Option<Vec<u8>> {
    display_name.map(|v| {
        make_index_key(
            project_ns(project_id, NAMESPACE).as_slice(),
            IDX_DISPLAY_NAME,
            v.as_str(),
        )
            .to_vec()
    })
}

macro_rules! attach_property {
    ($self:expr,$project_id:expr,$event_id:expr,$prop_id:expr,$prop:ident) => {{
        let tx = $self.db.transaction();

        let mut event = $self.get_by_id_(&tx, $project_id, $event_id)?;
        event.$prop = match event.$prop {
            None => Some(vec![$prop_id]),
            Some(props) => match props.iter().find(|x| $prop_id == **x) {
                None => Some([props, vec![$prop_id]].concat()),
                Some(_) => {
                    return Err(MetadataError::AlreadyExists(
                        "property already exist".to_string(),
                    ));
                }
            },
        };

        tx.put(
            make_data_value_key(project_ns($project_id, NAMESPACE).as_slice(), event.id),
            serialize(&event)?,
        )?;
        tx.commit()?;
        Ok(event)
    }};
}

macro_rules! detach_property {
    ($self:expr,$project_id:expr,$event_id:expr,$prop_id:expr,$prop:ident) => {{
        let tx = $self.db.transaction();
        let mut event = $self.get_by_id_(&tx, $project_id, $event_id)?;
        event.$prop = match event.$prop {
            None => {
                return Err(MetadataError::NotFound(
                    format!("property {} not found", $prop_id).to_string(),
                ));
            }
            Some(props) => match props.iter().find(|x| $prop_id == **x) {
                None => {
                    return Err(MetadataError::AlreadyExists(
                        "property already exist".to_string(),
                    ));
                }
                Some(_) => Some(props.into_iter().filter(|x| $prop_id != *x).collect()),
            },
        };

        tx.put(
            make_data_value_key(project_ns($project_id, NAMESPACE).as_slice(), event.id),
            serialize(&event)?,
        )?;
        tx.commit()?;
        Ok(event)
    }};
}

pub struct Events {
    db: Arc<TransactionDB>,
    dicts: Arc<Dictionaries>,
}

impl Events {
    pub fn new(db: Arc<TransactionDB>, dicts: Arc<Dictionaries>) -> Self {
        Events { db, dicts }
    }

    fn get_by_id_(
        &self,
        tx: &Transaction<TransactionDB>,
        project_id: u64,
        id: u64,
    ) -> Result<Event> {
        let key = make_data_value_key(project_ns(project_id, NAMESPACE).as_slice(), id);

        match tx.get(key)? {
            None => Err(MetadataError::NotFound(
                format!("event {id} not found").to_string(),
            )),
            Some(value) => Ok(deserialize(&value)?),
        }
    }

    fn create_(
        &self,
        tx: &Transaction<TransactionDB>,
        project_id: u64,
        req: CreateEventRequest,
    ) -> Result<Event> {
        let idx_keys = index_keys(project_id, &req.name, req.display_name.clone());

        check_insert_constraints(tx, idx_keys.as_ref())?;

        let created_at = Utc::now();
        let id = next_seq(
            tx,
            make_id_seq_key(project_ns(project_id, NAMESPACE).as_slice()),
        )?;

        let event = Event {
            id,
            created_at,
            updated_at: None,
            created_by: req.created_by,
            updated_by: None,
            project_id,
            tags: req.tags,
            name: req.name,
            display_name: req.display_name,
            description: req.description,
            status: req.status,
            is_system: req.is_system,
            event_properties: req.event_properties,
            custom_properties: req.custom_properties,
            user_properties: req.user_properties,
        };
        let data = serialize(&event)?;
        tx.put(
            make_data_value_key(project_ns(project_id, NAMESPACE).as_slice(), event.id),
            &data,
        )?;

        insert_index(tx, idx_keys.as_ref(), event.id)?;

        self.dicts
            ._get_key_or_create(tx, project_id, TABLE_EVENTS, COLUMN_EVENT, event.name.as_str())?;
        Ok(event)
    }

    fn get_by_name_(
        &self,
        tx: &Transaction<TransactionDB>,
        project_id: u64,
        name: &str,
    ) -> Result<Event> {
        let id = get_index(
            tx,
            make_index_key(project_ns(project_id, NAMESPACE).as_slice(), IDX_NAME, name),
            format!("event with name \"{}\" not found", name).as_str(),
        )?;

        self.get_by_id_(&tx, project_id, id)
    }

    pub fn create(&self, project_id: u64, req: CreateEventRequest) -> Result<Event> {
        let tx = self.db.transaction();
        let ret = self.create_(&tx, project_id, req);
        tx.commit()?;
        ret
    }

    pub fn get_or_create(&self, project_id: u64, req: CreateEventRequest) -> Result<Event> {
        let tx = self.db.transaction();
        match self.get_by_name_(&tx, project_id, req.name.as_str()) {
            Ok(event) => return Ok(event),
            Err(MetadataError::NotFound(_)) => {}
            other => return other,
        }

        let ret = self.create_(&tx, project_id, req);
        tx.commit()?;
        ret
    }

    pub fn get_by_id(&self, project_id: u64, id: u64) -> Result<Event> {
        let tx = self.db.transaction();

        self.get_by_id_(&tx, project_id, id)
    }

    pub fn get_by_name(&self, project_id: u64, name: &str) -> Result<Event> {
        let tx = self.db.transaction();
        self.get_by_name_(&tx, project_id, name)
    }

    pub fn list(&self, project_id: u64) -> Result<ListResponse<Event>> {
        let tx = self.db.transaction();

        let prefix = make_data_key(project_ns(project_id, NAMESPACE).as_slice());
        let iter = tx.prefix_iterator(prefix.clone());
        let mut list = vec![];
        for kv in iter {
            let (key, value) = kv?;
            // check if key contains the prefix
            if !from_utf8(&prefix).unwrap().is_prefix_of(from_utf8(&key).unwrap()) {
                break;
            }
            list.push(deserialize(&value)?);
        }


        Ok(ListResponse {
            data: list,
            meta: ResponseMetadata { next: None },
        })
    }

    pub fn update(&self, project_id: u64, event_id: u64, req: UpdateEventRequest) -> Result<Event> {
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
        if let OptionalProperty::Some(display_name) = &req.display_name {
            idx_keys.push(index_display_name_key(project_id, display_name.to_owned()));
            idx_prev_keys.push(index_display_name_key(project_id, prev_event.display_name));
            event.display_name = display_name.to_owned();
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
        if let OptionalProperty::Some(properties) = req.event_properties {
            event.event_properties = properties;
        }
        if let OptionalProperty::Some(properties) = req.user_properties {
            event.user_properties = properties;
        }
        if let OptionalProperty::Some(custom_properties) = req.custom_properties {
            event.custom_properties = custom_properties;
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

    pub fn try_attach_properties(
        &self,
        project_id: u64,
        event_id: u64,
        event_props: Vec<u64>,
    ) -> Result<()> {
        let tx = self.db.transaction();

        let mut event = self.get_by_id_(&tx, project_id, event_id)?;
        for prop in event_props {
            match &event.event_properties {
                None => {}
                Some(ex) => {
                    if ex.iter().any(|x| prop == *x) {
                        continue;
                    }
                }
            }
            event.event_properties = match event.event_properties {
                None => Some(vec![prop]),
                Some(props) => Some([props, vec![prop]].concat()),
            };
        }
        tx.put(
            make_data_value_key(project_ns(project_id, NAMESPACE).as_slice(), event.id),
            serialize(&event)?,
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn attach_event_property(
        &self,
        project_id: u64,
        event_id: u64,
        prop_id: u64,
    ) -> Result<Event> {
        attach_property!(self, project_id, event_id, prop_id, event_properties)
    }

    pub fn attach_user_property(
        &self,
        project_id: u64,
        event_id: u64,
        prop_id: u64,
    ) -> Result<Event> {
        attach_property!(self, project_id, event_id, prop_id, user_properties)
    }

    pub fn detach_event_property(
        &self,
        project_id: u64,
        event_id: u64,
        prop_id: u64,
    ) -> Result<Event> {
        detach_property!(self, project_id, event_id, prop_id, event_properties)
    }

    pub fn detach_user_property(
        &self,
        project_id: u64,
        event_id: u64,
        prop_id: u64,
    ) -> Result<Event> {
        detach_property!(self, project_id, event_id, prop_id, user_properties)
    }

    pub fn delete(&self, project_id: u64, id: u64) -> Result<Event> {
        let tx = self.db.transaction();
        let event = self.get_by_id_(&tx, project_id, id)?;
        tx.delete(make_data_value_key(
            project_ns(project_id, NAMESPACE).as_slice(),
            id,
        ))?;

        delete_index(
            &tx,
            index_keys(project_id, &event.name, event.display_name.clone()).as_ref(),
        )?;
        tx.commit()?;
        Ok(event)
    }

    pub fn next_record_sequence(&self, project_id: u64) -> Result<u64> {
        let tx = self.db.transaction();

        let id = next_seq(
            &tx,
            make_id_seq_key(project_ns(project_id, RECORDS_NAMESPACE).as_slice()),
        )?;

        tx.commit()?;
        Ok(id)
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
    pub id: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
    pub created_by: u64,
    pub updated_by: Option<u64>,
    pub project_id: u64,
    pub tags: Option<Vec<String>>,
    pub name: String,
    pub display_name: Option<String>,
    pub description: Option<String>,
    pub status: Status,
    pub is_system: bool,
    pub event_properties: Option<Vec<u64>>,
    pub user_properties: Option<Vec<u64>>,
    pub custom_properties: Option<Vec<u64>>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct CreateEventRequest {
    pub created_by: u64,
    pub tags: Option<Vec<String>>,
    pub name: String,
    pub display_name: Option<String>,
    pub description: Option<String>,
    pub status: Status,
    pub is_system: bool,
    pub event_properties: Option<Vec<u64>>,
    pub user_properties: Option<Vec<u64>>,
    pub custom_properties: Option<Vec<u64>>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct UpdateEventRequest {
    pub updated_by: u64,
    pub tags: OptionalProperty<Option<Vec<String>>>,
    pub name: OptionalProperty<String>,
    pub display_name: OptionalProperty<Option<String>>,
    pub description: OptionalProperty<Option<String>>,
    pub status: OptionalProperty<Status>,
    pub is_system: OptionalProperty<bool>,
    pub event_properties: OptionalProperty<Option<Vec<u64>>>,
    pub user_properties: OptionalProperty<Option<Vec<u64>>>,
    pub custom_properties: OptionalProperty<Option<Vec<u64>>>,
}

fn serialize(v: &Event) -> Result<Vec<u8>> {
    let tags = if let Some(tags) = &v.tags {
        tags.to_vec()
    } else { vec![] };

    let d = event::Event{
        id: v.id,
        created_at: v.created_at.timestamp(),
        created_by: v.created_by,
        updated_at: v.updated_at.map(|t| t.timestamp()),
        updated_by: v.updated_by,
        project_id: v.project_id,
        tags,
        name: v.name.clone(),
        display_name: v.display_name.clone(),
        description: v.description.clone(),
        status: match v.status {
            Status::Enabled => 1,
            Status::Disabled => 2,
        },
        is_system: v.is_system,
    };

    Ok(d.encode_to_vec())
}

fn deserialize(data: &[u8]) -> Result<Event> {
    let from = event::Event::decode(data.as_ref())?;

    Ok(
        Event{
            id: from.id,
            created_at: chrono::DateTime::from_timestamp(from.created_at, 0).unwrap(),
            created_by: from.created_by,
            updated_at: from.updated_at.map(|t| chrono::DateTime::from_timestamp(t, 0).unwrap()),
            updated_by: from.updated_by,
            project_id: from.project_id,
            tags: if from.tags.is_empty() { None } else { Some(from.tags) },
            name: from.name,
            display_name: from.display_name,
            description: from.description,
            status: match from.status {
                1 => Status::Enabled,
                2 => Status::Disabled,
                _ => unreachable!(),
            },
            is_system: from.is_system,
            event_properties: None,
            user_properties: None,
            custom_properties: None,
        }
    )
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;

    #[test]
    fn test_roundtrip() {
        let event = super::Event {
            id: 1,
            created_at: DateTime::from_timestamp(1, 0).unwrap(),
            updated_at: Some(DateTime::from_timestamp(2, 0)).unwrap(),
            created_by: 1,
            updated_by: Some(1),
            project_id: 1,
            tags: Some(vec!["tag1".to_string(), "tag2".to_string()]),
            name: "name".to_string(),
            display_name: Some("display_name".to_string()),
            description: Some("description".to_string()),
            status: super::Status::Disabled,
            is_system: true,
            event_properties: None,
            user_properties: None,
            custom_properties: None,
        };

        let data = super::serialize(&event).unwrap();
        let event2 = super::deserialize(&data).unwrap();

        assert_eq!(event, event2);
    }
}