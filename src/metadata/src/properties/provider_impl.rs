use std::sync::Arc;
use std::sync::RwLock;

use async_trait::async_trait;
use bincode::deserialize;
use bincode::serialize;
use chrono::Utc;
use common::types::{DType, OptionalProperty, TABLE_EVENTS};
use rocksdb::Transaction;
use rocksdb::TransactionDB;
use store::db::OptiDBImpl;

use crate::error;
use crate::error::MetadataError;
use crate::index::{check_insert_constraints, next_zero_seq};
use crate::index::check_update_constraints;
use crate::index::delete_index;
use crate::index::get_index;
use crate::index::insert_index;
use crate::index::next_seq;
use crate::index::update_index;
use crate::metadata::ListResponse;
use crate::properties::{CreatePropertyRequest, DictionaryType};
use crate::properties::Property;
use crate::properties::Provider;
use crate::properties::Type;
use crate::properties::UpdatePropertyRequest;
use crate::store::path_helpers::list;
use crate::store::path_helpers::make_data_value_key;
use crate::store::path_helpers::make_id_seq_key;
use crate::store::path_helpers::make_index_key;
use crate::store::path_helpers::org_proj_ns;
use crate::Result;

const IDX_NAME: &[u8] = b"name";
const IDX_DISPLAY_NAME: &[u8] = b"display_name";

fn index_keys(
    organization_id: u64,
    project_id: u64,
    typ: &Type,
    name: &str,
    display_name: Option<String>,
) -> Vec<Option<Vec<u8>>> {
    [
        index_name_key(organization_id, project_id, typ, name),
        index_display_name_key(organization_id, project_id, typ, display_name),
    ]
        .to_vec()
}

fn index_name_key(
    organization_id: u64,
    project_id: u64,
    typ: &Type,
    name: &str,
) -> Option<Vec<u8>> {
    Some(
        make_index_key(
            org_proj_ns(organization_id, project_id, typ.path().as_bytes()).as_slice(),
            IDX_NAME,
            name,
        )
            .to_vec(),
    )
}

fn index_display_name_key(
    organization_id: u64,
    project_id: u64,
    typ: &Type,
    display_name: Option<String>,
) -> Option<Vec<u8>> {
    display_name.map(|v| {
        make_index_key(
            org_proj_ns(organization_id, project_id, typ.path().as_bytes()).as_slice(),
            IDX_DISPLAY_NAME,
            v.as_str(),
        )
            .to_vec()
    })
}

pub struct ProviderImpl {
    db: Arc<TransactionDB>,
    optiDb: Arc<OptiDBImpl>,
    typ: Type,
}

impl ProviderImpl {
    pub fn new_user(db: Arc<TransactionDB>, optiDb: Arc<OptiDBImpl>) -> Self {
        ProviderImpl {
            db,
            optiDb,
            typ: Type::User,
        }
    }

    pub fn new_event(db: Arc<TransactionDB>, optiDb: Arc<OptiDBImpl>) -> Self {
        ProviderImpl {
            db,
            optiDb,
            typ: Type::Event,
        }
    }

    pub fn new_system(db: Arc<TransactionDB>, optiDb: Arc<OptiDBImpl>) -> Self {
        ProviderImpl {
            db,
            optiDb,
            typ: Type::System,
        }
    }

    fn _get_by_name(
        &self,
        tx: &Transaction<TransactionDB>,
        organization_id: u64,
        project_id: u64,
        name: &str,
    ) -> Result<Property> {
        let data = get_index(
            &tx,
            make_index_key(
                org_proj_ns(organization_id, project_id, self.typ.path().as_bytes()).as_slice(),
                IDX_NAME,
                name,
            ),
        )?;

        Ok(deserialize(&data)?)
    }

    fn _get_by_id(
        &self,
        tx: &Transaction<TransactionDB>,
        organization_id: u64,
        project_id: u64,
        id: u64,
    ) -> Result<Property> {
        let key = make_data_value_key(
            org_proj_ns(organization_id, project_id, self.typ.path().as_bytes()).as_slice(),
            id,
        );

        match tx.get(key)? {
            None => Err(MetadataError::NotFound("property not found".to_string())),
            Some(value) => Ok(deserialize(&value)?),
        }
    }

    fn _create(
        &self,
        tx: &Transaction<TransactionDB>,
        organization_id: u64,
        project_id: u64,
        req: CreatePropertyRequest,
    ) -> Result<Property> {
        let idx_keys = index_keys(
            organization_id,
            project_id,
            &self.typ,
            &req.name,
            req.display_name.clone(),
        );

        check_insert_constraints(&tx, idx_keys.as_ref())?;

        let id = next_seq(
            &tx,
            make_id_seq_key(
                org_proj_ns(organization_id, project_id, self.typ.path().as_bytes()).as_slice(),
            ),
        )?;

        let order = next_zero_seq(
            &tx,
            make_id_seq_key(org_proj_ns(organization_id, project_id, format!("{}/{}", self.typ.order_path(),req.data_type.short_name()).as_bytes()).as_slice()),
        )?;
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
            order,
            typ: req.typ,
            data_type: req.data_type.clone(),
            status: req.status,
            nullable: req.nullable,
            is_array: req.is_array,
            is_dictionary: req.is_dictionary,
            dictionary_type: req.dictionary_type.clone(),
            is_system: req.is_system,
        };

        let data = serialize(&prop)?;
        tx.put(
            make_data_value_key(
                org_proj_ns(organization_id, project_id, self.typ.path().as_bytes()).as_slice(),
                prop.id,
            ),
            &data,
        )?;


        insert_index(&tx, idx_keys.as_ref(), &data)?;

        let dt = if let Some(dt) = &req.dictionary_type {
            match dt {
                DictionaryType::Int8 => DType::Int8,
                DictionaryType::Int16 => DType::Int16,
                DictionaryType::Int32 => DType::Int32,
                DictionaryType::Int64 => DType::Int64,
            }
        } else {
            req.data_type.clone()
        };
        self.optiDb.add_field(TABLE_EVENTS, prop.column_name().as_str(), dt, req.nullable)?;
        Ok(prop)
    }
}

impl Provider for ProviderImpl {
    fn create(
        &self,
        organization_id: u64,
        project_id: u64,
        req: CreatePropertyRequest,
    ) -> Result<Property> {
        let tx = self.db.transaction();
        let ret = self._create(&tx, organization_id, project_id, req)?;
        tx.commit()?;

        Ok(ret)
    }

    fn get_or_create(
        &self,
        organization_id: u64,
        project_id: u64,
        req: CreatePropertyRequest,
    ) -> Result<Property> {
        let tx = self.db.transaction();
        match self._get_by_name(&tx, organization_id, project_id, req.name.as_str()) {
            Ok(event) => return Ok(event),
            Err(MetadataError::NotFound(_)) => {}
            Err(err) => return Err(err),
        }
        let ret = self._create(&tx, organization_id, project_id, req)?;

        tx.commit()?;

        Ok(ret)
    }

    fn get_by_id(&self, organization_id: u64, project_id: u64, id: u64) -> Result<Property> {
        let tx = self.db.transaction();
        self._get_by_id(&tx, organization_id, project_id, id)
    }

    fn get_by_name(&self, organization_id: u64, project_id: u64, name: &str) -> Result<Property> {
        let tx = self.db.transaction();
        self._get_by_name(&tx, organization_id, project_id, name)
    }

    fn list(&self, organization_id: u64, project_id: u64) -> Result<ListResponse<Property>> {
        let tx = self.db.transaction();
        list(
            &tx,
            org_proj_ns(organization_id, project_id, self.typ.path().as_bytes()).as_slice(),
        )
    }

    fn update(
        &self,
        organization_id: u64,
        project_id: u64,
        property_id: u64,
        req: UpdatePropertyRequest,
    ) -> Result<Property> {
        let tx = self.db.transaction();

        let prev_prop = self.get_by_id(organization_id, project_id, property_id)?;
        let mut prop = prev_prop.clone();

        let mut idx_keys: Vec<Option<Vec<u8>>> = Vec::new();
        let mut idx_prev_keys: Vec<Option<Vec<u8>>> = Vec::new();
        if let OptionalProperty::Some(name) = &req.name {
            idx_keys.push(index_name_key(
                organization_id,
                project_id,
                &self.typ,
                name.as_str(),
            ));
            idx_prev_keys.push(index_name_key(
                organization_id,
                project_id,
                &self.typ,
                prev_prop.name.as_str(),
            ));
            prop.name = name.to_owned();
        }
        if let OptionalProperty::Some(display_name) = &req.display_name {
            idx_keys.push(index_display_name_key(
                organization_id,
                project_id,
                &self.typ,
                display_name.clone(),
            ));
            idx_prev_keys.push(index_display_name_key(
                organization_id,
                project_id,
                &self.typ,
                prev_prop.display_name,
            ));
            prop.display_name = display_name.to_owned();
        }
        check_update_constraints(&tx, idx_keys.as_ref(), idx_prev_keys.as_ref())?;

        prop.updated_at = Some(Utc::now());
        prop.updated_by = Some(req.updated_by);
        if let OptionalProperty::Some(tags) = req.tags {
            prop.tags = tags;
        }
        if let OptionalProperty::Some(description) = req.description {
            prop.description = description;
        }
        if let OptionalProperty::Some(typ) = req.typ {
            prop.typ = typ;
        }

        if let OptionalProperty::Some(typ) = req.data_type {
            prop.data_type = typ;
        }
        if let OptionalProperty::Some(status) = req.status {
            prop.status = status;
        }
        if let OptionalProperty::Some(is_system) = req.is_system {
            prop.is_system = is_system;
        }
        if let OptionalProperty::Some(nullable) = req.nullable {
            prop.nullable = nullable;
        }
        if let OptionalProperty::Some(is_array) = req.is_array {
            prop.is_array = is_array;
        }
        if let OptionalProperty::Some(is_dictionary) = req.is_dictionary {
            prop.is_dictionary = is_dictionary;
        }
        if let OptionalProperty::Some(dictionary_type) = req.dictionary_type {
            prop.dictionary_type = dictionary_type;
        }

        let data = serialize(&prop)?;
        tx.put(
            make_data_value_key(
                org_proj_ns(organization_id, project_id, self.typ.path().as_bytes()).as_slice(),
                prop.id,
            ),
            &data,
        )?;

        update_index(&tx, idx_keys.as_ref(), idx_prev_keys.as_ref(), &data)?;
        tx.commit()?;
        Ok(prop)
    }

    fn delete(&self, organization_id: u64, project_id: u64, id: u64) -> Result<Property> {
        let tx = self.db.transaction();
        let prop = self._get_by_id(&tx, organization_id, project_id, id)?;
        tx.delete(make_data_value_key(
            org_proj_ns(organization_id, project_id, self.typ.path().as_bytes()).as_slice(),
            id,
        ))?;

        delete_index(
            &tx,
            index_keys(
                organization_id,
                project_id,
                &self.typ,
                &prop.name,
                prop.display_name.clone(),
            )
                .as_ref(),
        )?;
        tx.commit()?;
        Ok(prop)
    }
}
