use std::str::from_utf8;
use std::str::pattern::Pattern;
use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use common::query::EventRef;
use common::query::PropValueFilter;
use common::query::PropValueOperation;
use common::query::PropertyRef;
use common::types::OptionalProperty;
use common::DECIMAL_PRECISION;
use common::DECIMAL_SCALE;
use datafusion_common::ScalarValue;
use prost::Message;
use rocksdb::Transaction;
use rocksdb::TransactionDB;
use serde::Deserialize;
use serde::Serialize;

use crate::custom_event;
use crate::error::MetadataError;
use crate::events::Events;
use crate::index::check_insert_constraints;
use crate::index::check_update_constraints;
use crate::index::delete_index;
use crate::index::get_index;
use crate::index::insert_index;
use crate::index::next_seq;
use crate::index::update_index;
use crate::make_data_key;
use crate::make_data_value_key;
use crate::make_id_seq_key;
use crate::make_index_key;
use crate::metadata::ListResponse;
use crate::metadata::ResponseMetadata;
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
            data,
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

        let prefix = make_data_key(project_ns(project_id, NAMESPACE).as_slice());

        let iter = tx.prefix_iterator(prefix.clone());
        let mut list = vec![];
        for kv in iter {
            let (key, value) = kv?;
            // check if key contains the prefix
            if !from_utf8(&prefix)
                .unwrap()
                .is_prefix_of(from_utf8(&key).unwrap())
            {
                break;
            }
            list.push(deserialize(&value)?);
        }

        Ok(ListResponse {
            data: list,
            meta: ResponseMetadata { next: None },
        })
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
            event.name.clone_from(name);
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
            data,
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
    pub filters: Option<Vec<PropValueFilter>>,
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

fn serialize(v: &CustomEvent) -> Result<Vec<u8>> {
    let status = match v.status {
        Status::Enabled => custom_event::Status::Enabled as i32,
        Status::Disabled => custom_event::Status::Disabled as i32,
    };

    let mut events = vec![];
    for e in &v.events {
        let mut ce = custom_event::Event {
            regular_name: None,
            regular: None,
            custom: None,
            filters: vec![],
        };
        match &e.event {
            EventRef::RegularName(name) => ce.regular_name = Some(name.to_owned()),
            EventRef::Regular(id) => ce.regular = Some(*id),
            EventRef::Custom(id) => ce.custom = Some(*id),
        }

        if let Some(filters) = &e.filters {
            for filter in filters {
                match filter {
                    PropValueFilter::Property {
                        property,
                        operation,
                        value,
                    } => {
                        let mut pvf = custom_event::PropValueFilter {
                            property_name: None,
                            property_group: None,
                            property_custom_id: None,
                            operation: 0,
                            value: vec![],
                        };
                        match property {
                            PropertyRef::Group(n, gid) => {
                                pvf.property_name = Some(n.to_owned());
                                pvf.property_group = Some(*gid as u64);
                            }
                            PropertyRef::Event(n) => {
                                pvf.property_name = Some(n.to_owned());
                            }
                            PropertyRef::Custom(id) => pvf.property_custom_id = Some(*id),
                        }

                        let op = match operation {
                            PropValueOperation::Eq => custom_event::PropValueOperation::Eq,
                            PropValueOperation::Neq => custom_event::PropValueOperation::Neq,
                            PropValueOperation::Gt => custom_event::PropValueOperation::Gt,
                            PropValueOperation::Gte => custom_event::PropValueOperation::Gte,
                            PropValueOperation::Lt => custom_event::PropValueOperation::Lt,
                            PropValueOperation::Lte => custom_event::PropValueOperation::Lte,
                            PropValueOperation::True => custom_event::PropValueOperation::True,
                            PropValueOperation::False => custom_event::PropValueOperation::False,
                            PropValueOperation::Exists => custom_event::PropValueOperation::Exists,
                            PropValueOperation::Empty => custom_event::PropValueOperation::Empty,
                            PropValueOperation::Like => custom_event::PropValueOperation::Like,
                            PropValueOperation::NotLike => {
                                custom_event::PropValueOperation::NotLike
                            }
                            PropValueOperation::Regex => custom_event::PropValueOperation::Regex,
                            PropValueOperation::NotRegex => {
                                custom_event::PropValueOperation::NotRegex
                            }
                        };
                        pvf.operation = op as i32;

                        let v = if let Some(v) = value {
                            v.iter()
                                .map(|v| match v {
                                    ScalarValue::Boolean(v) => custom_event::Value {
                                        string: None,
                                        int8: None,
                                        int16: None,
                                        int32: None,
                                        int64: None,
                                        decimal: None,
                                        bool: v.map(|v| v as u32),
                                        timestamp: None,
                                    },
                                    ScalarValue::Decimal128(v, _, _) => custom_event::Value {
                                        string: None,
                                        int8: None,
                                        int16: None,
                                        int32: None,
                                        int64: None,
                                        decimal: v.map(|v| v.to_le_bytes().to_vec()),
                                        bool: None,
                                        timestamp: None,
                                    },
                                    ScalarValue::Int8(v) => custom_event::Value {
                                        string: None,
                                        int8: v.map(|v| v as i64),
                                        int16: None,
                                        int32: None,
                                        int64: None,
                                        decimal: None,
                                        bool: None,
                                        timestamp: None,
                                    },
                                    ScalarValue::Int16(v) => custom_event::Value {
                                        string: None,
                                        int8: None,
                                        int16: v.map(|v| v as i64),
                                        int32: None,
                                        int64: None,
                                        decimal: None,
                                        bool: None,
                                        timestamp: None,
                                    },
                                    ScalarValue::Int32(v) => custom_event::Value {
                                        string: None,
                                        int8: None,
                                        int16: None,
                                        int32: v.map(|v| v as i64),
                                        int64: None,
                                        decimal: None,
                                        bool: None,
                                        timestamp: None,
                                    },
                                    ScalarValue::Int64(v) => custom_event::Value {
                                        string: None,
                                        int8: None,
                                        int16: None,
                                        int32: None,
                                        int64: v.to_owned(),
                                        decimal: None,
                                        bool: None,
                                        timestamp: None,
                                    },
                                    ScalarValue::Utf8(v) => custom_event::Value {
                                        string: v.to_owned(),
                                        int8: None,
                                        int16: None,
                                        int32: None,
                                        int64: None,
                                        decimal: None,
                                        bool: None,
                                        timestamp: None,
                                    },
                                    ScalarValue::TimestampMillisecond(v, _) => {
                                        custom_event::Value {
                                            string: None,
                                            int8: None,
                                            int16: None,
                                            int32: None,
                                            int64: None,
                                            decimal: None,
                                            bool: None,
                                            timestamp: v.to_owned(),
                                        }
                                    }
                                    _ => unimplemented!(),
                                })
                                .collect::<Vec<_>>()
                        } else {
                            vec![]
                        };
                        pvf.value = v;
                        ce.filters.push(pvf);
                    }
                }
            }
        }
        events.push(ce);
    }

    let tags = if let Some(tags) = &v.tags {
        tags.to_vec()
    } else {
        vec![]
    };
    let ce = custom_event::CustomEvent {
        id: v.id,
        created_at: v.created_at.timestamp(),
        created_by: v.created_by,
        updated_at: v.updated_at.map(|t| t.timestamp()),
        updated_by: v.updated_by,
        project_id: v.project_id,
        tags,
        name: v.name.clone(),
        description: v.description.clone(),
        status,
        is_system: v.is_system,
        events,
    };

    Ok(ce.encode_to_vec())
}

fn deserialize(data: &[u8]) -> Result<CustomEvent> {
    let from = custom_event::CustomEvent::decode(data)?;

    let mut events = vec![];

    for event in from.events {
        let er = if let Some(f) = event.regular {
            EventRef::Regular(f)
        } else if let Some(f) = event.regular_name {
            EventRef::RegularName(f)
        } else {
            EventRef::Custom(event.custom.unwrap())
        };
        let mut filters = vec![];

        for filter in &event.filters {
            let prop_ref = if let Some(n) = &filter.property_name {
                if let Some(g) = filter.property_group {
                    PropertyRef::Group(n.to_owned(), g as usize)
                } else {
                    PropertyRef::Event(n.to_owned())
                }
            } else {
                PropertyRef::Custom(filter.property_custom_id.unwrap())
            };

            let op = match filter.operation {
                1 => PropValueOperation::Eq,
                2 => PropValueOperation::Neq,
                3 => PropValueOperation::Gt,
                4 => PropValueOperation::Gte,
                5 => PropValueOperation::Lt,
                6 => PropValueOperation::Lte,
                7 => PropValueOperation::True,
                8 => PropValueOperation::False,
                9 => PropValueOperation::Exists,
                10 => PropValueOperation::Empty,
                11 => PropValueOperation::Like,
                12 => PropValueOperation::NotLike,
                13 => PropValueOperation::Regex,
                14 => PropValueOperation::NotRegex,
                _ => unimplemented!(),
            };

            let mut vals = vec![];
            for value in filter.value.clone() {
                let v = if let Some(v) = value.string {
                    ScalarValue::Utf8(Some(v))
                } else if let Some(v) = value.int8 {
                    ScalarValue::Int8(Some(v as i8))
                } else if let Some(v) = value.int16 {
                    ScalarValue::Int16(Some(v as i16))
                } else if let Some(v) = value.int32 {
                    ScalarValue::Int32(Some(v as i32))
                } else if let Some(v) = value.int64 {
                    ScalarValue::Int64(Some(v))
                } else if let Some(v) = value.decimal {
                    ScalarValue::Decimal128(
                        Some(i128::from_le_bytes(v.as_slice().try_into().unwrap())),
                        DECIMAL_PRECISION,
                        DECIMAL_SCALE,
                    )
                } else if let Some(v) = value.bool {
                    ScalarValue::Boolean(Some(v != 0))
                } else if let Some(v) = value.timestamp {
                    ScalarValue::TimestampMillisecond(Some(v), None)
                } else {
                    unimplemented!()
                };
                vals.push(v);
            }

            filters.push(PropValueFilter::Property {
                property: prop_ref.clone(),
                operation: op,
                value: Some(vals),
            });
        }
        let event = Event {
            event: er,
            filters: if filters.is_empty() {
                None
            } else {
                Some(filters)
            },
        };
        events.push(event);
    }

    let e = CustomEvent {
        id: from.id,
        created_at: DateTime::from_timestamp(from.created_at, 0).unwrap(),
        created_by: from.created_by,
        updated_at: from
            .updated_at
            .map(|t| DateTime::from_timestamp(t, 0).unwrap()),
        updated_by: from.updated_by,
        project_id: from.project_id,
        tags: if from.tags.is_empty() {
            None
        } else {
            Some(from.tags)
        },
        name: from.name,
        description: from.description,
        status: match from.status {
            1 => Status::Enabled,
            2 => Status::Disabled,
            _ => unimplemented!(),
        },
        is_system: from.is_system,
        events,
    };

    Ok(e)
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;
    use common::query::EventRef;
    use common::query::PropValueFilter;
    use common::query::PropValueOperation;
    use common::query::PropertyRef;
    use common::DECIMAL_PRECISION;
    use common::DECIMAL_SCALE;
    use datafusion_common::ScalarValue;

    use crate::custom_events::deserialize;
    use crate::custom_events::serialize;
    use crate::custom_events::CustomEvent;
    use crate::custom_events::Event;
    use crate::custom_events::Status;

    #[test]
    fn test_roundtrip() {
        let event = CustomEvent {
            id: 1,
            created_at: DateTime::from_timestamp(1, 0).unwrap(),
            updated_at: DateTime::from_timestamp(2, 0),
            created_by: 1,
            updated_by: Some(2),
            project_id: 3,
            tags: Some(vec!["tag1".to_string(), "tag2".to_string()]),
            name: "name".to_string(),
            description: Some("description".to_string()),
            status: Status::Enabled,
            is_system: true,
            events: vec![
                Event {
                    event: EventRef::Regular(1),
                    filters: Some(vec![PropValueFilter::Property {
                        property: PropertyRef::Group("group".to_string(), 1),
                        operation: PropValueOperation::Eq,
                        value: Some(vec![ScalarValue::Utf8(Some("value".to_string()))]),
                    }]),
                },
                Event {
                    event: EventRef::Custom(1),
                    filters: None,
                },
                Event {
                    event: EventRef::RegularName("sdf".to_string()),
                    filters: Some(vec![
                        PropValueFilter::Property {
                            property: PropertyRef::Event("e".to_string()),
                            operation: PropValueOperation::Eq,
                            value: Some(vec![ScalarValue::Decimal128(
                                Some(123),
                                DECIMAL_PRECISION,
                                DECIMAL_SCALE,
                            )]),
                        },
                        PropValueFilter::Property {
                            property: PropertyRef::Event("e".to_string()),
                            operation: PropValueOperation::Eq,
                            value: Some(vec![ScalarValue::Boolean(Some(true))]),
                        },
                    ]),
                },
            ],
        };

        let data = serialize(&event).unwrap();
        let event2 = deserialize(&data).unwrap();
        assert_eq!(event, event2);
    }
}
