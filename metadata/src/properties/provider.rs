use crate::error::Error;
use crate::metadata::{list, ListResponse};
use crate::properties::types::{CreatePropertyRequest, Property, UpdatePropertyRequest};
use crate::store::index::hash_map::HashMap;
use crate::store::{make_data_value_key, make_id_seq_key, make_index_key, Store};
use crate::Result;
use bincode::{deserialize, serialize};
use chrono::Utc;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone)]
pub enum Namespace {
    Event,
    User,
}

const IDX_NAME: &[u8] = b"name";
const IDX_DISPLAY_NAME: &[u8] = b"display_name";

impl Namespace {
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            Namespace::Event => "event_properties".as_bytes(),
            Namespace::User => "user_properties".as_bytes(),
        }
    }
}

fn index_keys(
    organization_id: u64,
    project_id: u64,
    ns: &Namespace,
    name: &str,
    display_name: Option<String>,
) -> Vec<Option<Vec<u8>>> {
    [
        index_name_key(organization_id, project_id, ns, name),
        index_display_name_key(organization_id, project_id, ns, display_name),
    ]
    .to_vec()
}

fn index_name_key(
    organization_id: u64,
    project_id: u64,
    ns: &Namespace,
    name: &str,
) -> Option<Vec<u8>> {
    Some(make_index_key(organization_id, project_id, ns.as_bytes(), IDX_NAME, name).to_vec())
}

fn index_display_name_key(
    organization_id: u64,
    project_id: u64,
    ns: &Namespace,
    display_name: Option<String>,
) -> Option<Vec<u8>> {
    display_name.map(|v| {
        make_index_key(
            organization_id,
            project_id,
            ns.as_bytes(),
            IDX_DISPLAY_NAME,
            v.as_str(),
        )
        .to_vec()
    })
}

pub struct Provider {
    store: Arc<Store>,
    idx: HashMap,
    guard: RwLock<()>,
    ns: Namespace,
}

impl Provider {
    pub fn new_user(kv: Arc<Store>) -> Self {
        Provider {
            store: kv.clone(),
            idx: HashMap::new(kv),
            guard: RwLock::new(()),
            ns: Namespace::User,
        }
    }

    pub fn new_event(kv: Arc<Store>) -> Self {
        Provider {
            store: kv.clone(),
            idx: HashMap::new(kv),
            guard: RwLock::new(()),
            ns: Namespace::Event,
        }
    }

    pub async fn create(
        &self,
        organization_id: u64,
        project_id: u64,
        req: CreatePropertyRequest,
    ) -> Result<Property> {
        let _guard = self.guard.write().await;
        self._create(organization_id, project_id, req).await
    }

    pub async fn _create(
        &self,
        organization_id: u64,
        project_id: u64,
        req: CreatePropertyRequest,
    ) -> Result<Property> {
        let idx_keys = index_keys(
            organization_id,
            project_id,
            &self.ns,
            &req.name,
            req.display_name.clone(),
        );
        self.idx.check_insert_constraints(idx_keys.as_ref()).await?;

        let id = self
            .store
            .next_seq(make_id_seq_key(
                organization_id,
                project_id,
                self.ns.as_bytes(),
            ))
            .await?;
        let created_at = Utc::now();

        let prop = Property {
            id,
            created_at,
            updated_at: None,
            created_by: req.created_by,
            updated_by: None,
            project_id,
            tags: req.tags,
            name: req.name,
            description: req.description,
            display_name: req.display_name,
            typ: req.typ,
            status: req.status,
            nullable: req.nullable,
            is_array: req.is_array,
            is_dictionary: req.is_dictionary,
            dictionary_type: req.dictionary_type,
            is_system: req.is_system,
        };

        let data = serialize(&prop)?;
        self.store
            .put(
                make_data_value_key(organization_id, project_id, self.ns.as_bytes(), prop.id),
                &data,
            )
            .await?;

        self.idx.insert(idx_keys.as_ref(), &data).await?;
        Ok(prop)
    }

    pub async fn get_or_create(
        &self,
        organization_id: u64,
        project_id: u64,
        req: CreatePropertyRequest,
    ) -> Result<Property> {
        let _guard = self.guard.write().await;
        match self
            ._get_by_name(organization_id, project_id, req.name.as_str())
            .await
        {
            Ok(prop) => return Ok(prop),
            Err(Error::KeyNotFound(_)) => {}
            Err(err) => return Err(err),
        }

        self._create(organization_id, project_id, req).await
    }

    pub async fn get_by_id(
        &self,
        organization_id: u64,
        project_id: u64,
        id: u64,
    ) -> Result<Property> {
        let key = make_data_value_key(organization_id, project_id, self.ns.as_bytes(), id);

        match self.store.get(&key).await? {
            None => Err(Error::KeyNotFound(String::from_utf8(key)?)),
            Some(value) => Ok(deserialize(&value)?),
        }
    }

    pub async fn get_by_name(
        &self,
        organization_id: u64,
        project_id: u64,
        name: &str,
    ) -> Result<Property> {
        let _guard = self.guard.read().await;
        self._get_by_name(organization_id, project_id, name).await
    }

    pub async fn _get_by_name(
        &self,
        organization_id: u64,
        project_id: u64,
        name: &str,
    ) -> Result<Property> {
        let data = self
            .idx
            .get(make_index_key(
                organization_id,
                project_id,
                self.ns.as_bytes(),
                IDX_NAME,
                name,
            ))
            .await?;

        Ok(deserialize(&data)?)
    }

    pub async fn list(
        &self,
        organization_id: u64,
        project_id: u64,
    ) -> Result<ListResponse<Property>> {
        list(
            self.store.clone(),
            organization_id,
            project_id,
            self.ns.as_bytes(),
        )
        .await
    }

    pub async fn update(
        &self,
        organization_id: u64,
        project_id: u64,
        property_id: u64,
        req: UpdatePropertyRequest,
    ) -> Result<Property> {
        let _guard = self.guard.write().await;

        let prev_prop = self
            .get_by_id(organization_id, project_id, property_id)
            .await?;
        let mut prop = prev_prop.clone();

        let mut idx_keys: Vec<Option<Vec<u8>>> = Vec::new();
        let mut idx_prev_keys: Vec<Option<Vec<u8>>> = Vec::new();
        if let Some(name) = req.name {
            idx_keys.push(index_name_key(
                organization_id,
                project_id,
                &self.ns,
                name.as_str(),
            ));
            idx_prev_keys.push(index_name_key(
                organization_id,
                project_id,
                &self.ns,
                prev_prop.name.as_str(),
            ));
            prop.name = name;
        }
        if let Some(display_name) = req.display_name {
            idx_keys.push(index_display_name_key(
                organization_id,
                project_id,
                &self.ns,
                display_name.clone(),
            ));
            idx_prev_keys.push(index_display_name_key(
                organization_id,
                project_id,
                &self.ns,
                prev_prop.display_name,
            ));
            prop.display_name = display_name;
        }
        self.idx
            .check_update_constraints(idx_keys.as_ref(), idx_prev_keys.as_ref())
            .await?;

        prop.updated_at = Some(Utc::now());
        prop.updated_by = Some(req.updated_by);
        if let Some(tags) = req.tags {
            prop.tags = tags;
        }
        if let Some(description) = req.description {
            prop.description = description;
        }
        if let Some(typ) = req.typ {
            prop.typ = typ;
        }
        if let Some(status) = req.status {
            prop.status = status;
        }
        if let Some(is_system) = req.is_system {
            prop.is_system = is_system;
        }
        if let Some(nullable) = req.nullable {
            prop.nullable = nullable;
        }
        if let Some(is_array) = req.is_array {
            prop.is_array = is_array;
        }
        if let Some(is_dictionary) = req.is_dictionary {
            prop.is_dictionary = is_dictionary;
        }
        if let Some(dictionary_type) = req.dictionary_type {
            prop.dictionary_type = dictionary_type;
        }

        let data = serialize(&prop)?;
        self.store
            .put(
                make_data_value_key(organization_id, project_id, self.ns.as_bytes(), prop.id),
                &data,
            )
            .await?;

        self.idx
            .update(idx_keys.as_ref(), idx_prev_keys.as_ref(), &data)
            .await?;

        Ok(prop)
    }

    pub async fn delete(&self, organization_id: u64, project_id: u64, id: u64) -> Result<Property> {
        let _guard = self.guard.write().await;
        let prop = self.get_by_id(organization_id, project_id, id).await?;
        self.store
            .delete(make_data_value_key(
                organization_id,
                project_id,
                self.ns.as_bytes(),
                id,
            ))
            .await?;

        self.idx
            .delete(
                index_keys(
                    organization_id,
                    project_id,
                    &self.ns,
                    &prop.name,
                    prop.display_name.clone(),
                )
                .as_ref(),
            )
            .await?;
        Ok(prop)
    }
}
