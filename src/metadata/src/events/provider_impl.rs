use std::sync::Arc;
use std::sync::RwLock;

use async_trait::async_trait;
use bincode::deserialize;
use bincode::serialize;
use chrono::Utc;
use common::types::OptionalProperty;
use rocksdb::Transaction;
use rocksdb::TransactionDB;

use crate::error;
use crate::error::MetadataError;
use crate::events::CreateEventRequest;
use crate::events::Event;
use crate::events::Provider;
use crate::events::UpdateEventRequest;
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
use crate::Result;

const NAMESPACE: &[u8] = b"events";
const RECORDS_NAMESPACE: &[u8] = b"events/records";
const IDX_NAME: &[u8] = b"name";
const IDX_DISPLAY_NAME: &[u8] = b"display_name";

fn index_keys(
    organization_id: u64,
    project_id: u64,
    name: &str,
    display_name: Option<String>,
) -> Vec<Option<Vec<u8>>> {
    [
        index_name_key(organization_id, project_id, name),
        index_display_name_key(organization_id, project_id, display_name),
    ]
    .to_vec()
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

fn index_display_name_key(
    organization_id: u64,
    project_id: u64,
    display_name: Option<String>,
) -> Option<Vec<u8>> {
    display_name.map(|v| {
        make_index_key(
            org_proj_ns(organization_id, project_id, NAMESPACE).as_slice(),
            IDX_DISPLAY_NAME,
            v.as_str(),
        )
        .to_vec()
    })
}

pub struct ProviderImpl {
    db: Arc<TransactionDB>,
}

impl ProviderImpl {
    pub fn new(db: Arc<TransactionDB>) -> Self {
        ProviderImpl { db }
    }

    fn _get_by_id(
        &self,
        tx: &Transaction<TransactionDB>,
        organization_id: u64,
        project_id: u64,
        id: u64,
    ) -> Result<Event> {
        let key = make_data_value_key(
            org_proj_ns(organization_id, project_id, NAMESPACE).as_slice(),
            id,
        );

        match tx.get(key)? {
            None => Err(MetadataError::NotFound("event not found".to_string())),
            Some(value) => Ok(deserialize(&value)?),
        }
    }

    fn _create(
        &self,
        tx: &Transaction<TransactionDB>,
        organization_id: u64,
        project_id: u64,
        req: CreateEventRequest,
    ) -> Result<Event> {
        let idx_keys = index_keys(
            organization_id,
            project_id,
            &req.name,
            req.display_name.clone(),
        );

        check_insert_constraints(tx, idx_keys.as_ref())?;

        let created_at = Utc::now();
        let id = next_seq(
            tx,
            make_id_seq_key(org_proj_ns(organization_id, project_id, NAMESPACE).as_slice()),
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
            properties: req.properties,
            custom_properties: req.custom_properties,
        };
        let data = serialize(&event)?;
        tx.put(
            make_data_value_key(
                org_proj_ns(organization_id, project_id, NAMESPACE).as_slice(),
                event.id,
            ),
            &data,
        )?;

        insert_index(tx, idx_keys.as_ref(), &data)?;

        Ok(event)
    }

    fn _get_by_name(
        &self,
        tx: &Transaction<TransactionDB>,
        organization_id: u64,
        project_id: u64,
        name: &str,
    ) -> Result<Event> {
        let data = get_index(
            tx,
            make_index_key(
                org_proj_ns(organization_id, project_id, NAMESPACE).as_slice(),
                IDX_NAME,
                name,
            ),
        )?;

        Ok(deserialize(&data)?)
    }
}

impl Provider for ProviderImpl {
    fn create(
        &self,
        organization_id: u64,
        project_id: u64,
        req: CreateEventRequest,
    ) -> Result<Event> {
        let tx = self.db.transaction();
        let ret = self._create(&tx, organization_id, project_id, req);
        tx.commit()?;
        ret
    }

    fn get_or_create(
        &self,
        organization_id: u64,
        project_id: u64,
        req: CreateEventRequest,
    ) -> Result<Event> {
        let tx = self.db.transaction();
        match self._get_by_name(&tx, organization_id, project_id, req.name.as_str()) {
            Ok(event) => return Ok(event),
            Err(MetadataError::NotFound(_)) => {}
            other => return other,
        }

        let ret = self._create(&tx, organization_id, project_id, req);
        tx.commit()?;
        ret
    }

    fn get_by_id(&self, organization_id: u64, project_id: u64, id: u64) -> Result<Event> {
        let tx = self.db.transaction();

        self._get_by_id(&tx, organization_id, project_id, id)
    }

    fn get_by_name(&self, organization_id: u64, project_id: u64, name: &str) -> Result<Event> {
        let tx = self.db.transaction();
        self._get_by_name(&tx, organization_id, project_id, name)
    }

    fn list(&self, organization_id: u64, project_id: u64) -> Result<ListResponse<Event>> {
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
        req: UpdateEventRequest,
    ) -> Result<Event> {
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
        if let OptionalProperty::Some(display_name) = &req.display_name {
            idx_keys.push(index_display_name_key(
                organization_id,
                project_id,
                display_name.to_owned(),
            ));
            idx_prev_keys.push(index_display_name_key(
                organization_id,
                project_id,
                prev_event.display_name,
            ));
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
        if let OptionalProperty::Some(properties) = req.properties {
            event.properties = properties;
        }
        if let OptionalProperty::Some(custom_properties) = req.custom_properties {
            event.custom_properties = custom_properties;
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

    fn attach_property(
        &self,
        organization_id: u64,
        project_id: u64,
        event_id: u64,
        prop_id: u64,
    ) -> Result<Event> {
        let tx = self.db.transaction();

        let mut event = self._get_by_id(&tx, organization_id, project_id, event_id)?;
        event.properties = match event.properties {
            None => Some(vec![prop_id]),
            Some(props) => match props.iter().find(|x| prop_id == **x) {
                None => Some([props, vec![prop_id]].concat()),
                Some(_) => {
                    return Err(MetadataError::AlreadyExists(
                        "property already exist".to_string(),
                    ));
                }
            },
        };

        tx.put(
            make_data_value_key(
                org_proj_ns(organization_id, project_id, NAMESPACE).as_slice(),
                event.id,
            ),
            serialize(&event)?,
        )?;
        tx.commit()?;
        Ok(event)
    }

    fn detach_property(
        &self,
        organization_id: u64,
        project_id: u64,
        event_id: u64,
        prop_id: u64,
    ) -> Result<Event> {
        let tx = self.db.transaction();
        let mut event = self._get_by_id(&tx, organization_id, project_id, event_id)?;
        event.properties = match event.properties {
            None => {
                return Err(MetadataError::NotFound("property not found".to_string()));
            }
            Some(props) => match props.iter().find(|x| prop_id == **x) {
                None => {
                    return Err(MetadataError::AlreadyExists(
                        "property already exist".to_string(),
                    ));
                }
                Some(_) => Some(props.into_iter().filter(|x| prop_id != *x).collect()),
            },
        };

        tx.put(
            make_data_value_key(
                org_proj_ns(organization_id, project_id, NAMESPACE).as_slice(),
                event.id,
            ),
            serialize(&event)?,
        )?;
        tx.commit()?;
        Ok(event)
    }

    fn delete(&self, organization_id: u64, project_id: u64, id: u64) -> Result<Event> {
        let tx = self.db.transaction();
        let event = self._get_by_id(&tx, organization_id, project_id, id)?;
        tx.delete(make_data_value_key(
            org_proj_ns(organization_id, project_id, NAMESPACE).as_slice(),
            id,
        ))?;

        delete_index(
            &tx,
            index_keys(
                organization_id,
                project_id,
                &event.name,
                event.display_name.clone(),
            )
            .as_ref(),
        )?;
        tx.commit()?;
        Ok(event)
    }

    fn generate_record_id(&self, organization_id: u64, project_id: u64) -> Result<u64> {
        let tx = self.db.transaction();

        let id = next_seq(
            &tx,
            make_id_seq_key(org_proj_ns(organization_id, project_id, RECORDS_NAMESPACE).as_slice()),
        )?;

        Ok(id)
    }
}
