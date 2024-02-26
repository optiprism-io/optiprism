use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::RwLock;

use arrow::datatypes;
use bincode::deserialize;
use bincode::serialize;
use chrono::DateTime;
use chrono::Utc;
use common::types::DType;
use common::types::OptionalProperty;
use common::types::TABLE_EVENTS;
use convert_case::Case;
use convert_case::Casing;
use lru::LruCache;
use rocksdb::Transaction;
use rocksdb::TransactionDB;
use serde::Deserialize;
use serde::Serialize;
use storage::db::OptiDBImpl;
use storage::error::StoreError;

use crate::error::MetadataError;
use crate::index::check_insert_constraints;
use crate::index::check_update_constraints;
use crate::index::delete_index;
use crate::index::get_index;
use crate::index::insert_index;
use crate::index::next_seq;
use crate::index::next_zero_seq;
use crate::index::update_index;
use crate::list_data;
use crate::make_data_value_key;
use crate::make_id_seq_key;
use crate::make_index_key;
use crate::metadata::ListResponse;
use crate::project_ns;
use crate::Result;

const IDX_NAME: &[u8] = b"name";
const IDX_DISPLAY_NAME: &[u8] = b"display_name";

fn index_keys(
    project_id: u64,
    typ: &Type,
    name: &str,
    display_name: Option<String>,
) -> Vec<Option<Vec<u8>>> {
    [
        index_name_key(project_id, typ, name),
        index_display_name_key(project_id, typ, display_name),
    ]
    .to_vec()
}

fn index_name_key(project_id: u64, typ: &Type, name: &str) -> Option<Vec<u8>> {
    Some(
        make_index_key(
            project_ns(project_id, typ.path().as_bytes()).as_slice(),
            IDX_NAME,
            name,
        )
        .to_vec(),
    )
}

fn index_display_name_key(
    project_id: u64,
    typ: &Type,
    display_name: Option<String>,
) -> Option<Vec<u8>> {
    display_name.map(|v| {
        make_index_key(
            project_ns(project_id, typ.path().as_bytes()).as_slice(),
            IDX_DISPLAY_NAME,
            v.as_str(),
        )
        .to_vec()
    })
}

pub struct Properties {
    db: Arc<TransactionDB>,
    opti_db: Arc<OptiDBImpl>,
    id_cache: RwLock<LruCache<(u64, u64), Property>>,
    name_cache: RwLock<LruCache<(u64, String), Property>>,
    typ: Type,
}

impl Properties {
    pub fn new_user(db: Arc<TransactionDB>, opti_db: Arc<OptiDBImpl>) -> Self {
        let id_cache = RwLock::new(LruCache::new(
            NonZeroUsize::new(10 /* todo why 10? */).unwrap(),
        ));
        let name_cache = RwLock::new(LruCache::new(
            NonZeroUsize::new(10 /* todo why 10? */).unwrap(),
        ));
        Properties {
            db,
            opti_db,
            id_cache,
            name_cache,
            typ: Type::User,
        }
    }

    pub fn new_event(db: Arc<TransactionDB>, opti_db: Arc<OptiDBImpl>) -> Self {
        let id_cache = RwLock::new(LruCache::new(
            NonZeroUsize::new(10 /* todo why 10? */).unwrap(),
        ));
        let name_cache = RwLock::new(LruCache::new(
            NonZeroUsize::new(10 /* todo why 10? */).unwrap(),
        ));
        Properties {
            db,
            id_cache,
            name_cache,
            opti_db,
            typ: Type::Event,
        }
    }

    pub fn new_system(db: Arc<TransactionDB>, opti_db: Arc<OptiDBImpl>) -> Self {
        let id_cache = RwLock::new(LruCache::new(
            NonZeroUsize::new(10 /* todo why 10? */).unwrap(),
        ));
        let name_cache = RwLock::new(LruCache::new(
            NonZeroUsize::new(10 /* todo why 10? */).unwrap(),
        ));
        Properties {
            db,
            opti_db,
            id_cache,
            name_cache,
            typ: Type::System,
        }
    }

    fn get_by_name_(
        &self,
        tx: &Transaction<TransactionDB>,
        project_id: u64,
        name: &str,
    ) -> Result<Property> {
        if let Some(prop) = self
            .name_cache
            .write()
            .unwrap()
            .get(&(project_id, name.to_string()))
        {
            return Ok(prop.to_owned());
        }

        let idx_key = make_index_key(
            project_ns(project_id, self.typ.path().as_bytes()).as_slice(),
            IDX_NAME,
            name,
        );
        let data = get_index(
            tx,
            idx_key,
            format!("property with name \"{}\" not found", name).as_str(),
        )?;

        Ok(deserialize(&data)?)
    }

    fn get_by_id_(
        &self,
        tx: &Transaction<TransactionDB>,
        project_id: u64,
        id: u64,
    ) -> Result<Property> {
        if let Some(prop) = self.id_cache.write().unwrap().get(&(project_id, id)) {
            return Ok(prop.to_owned());
        }

        let key = make_data_value_key(
            project_ns(project_id, self.typ.path().as_bytes()).as_slice(),
            id,
        );

        match tx.get(key)? {
            None => Err(MetadataError::NotFound(
                format!("property {id} not found").to_string(),
            )),
            Some(value) => Ok(deserialize(&value)?),
        }
    }

    fn create_(
        &self,
        tx: &Transaction<TransactionDB>,
        project_id: u64,
        req: CreatePropertyRequest,
    ) -> Result<Property> {
        let project_id = if self.typ == Type::System {
            0
        } else {
            project_id
        };
        let idx_keys = index_keys(project_id, &self.typ, &req.name, req.display_name.clone());

        check_insert_constraints(tx, idx_keys.as_ref())?;

        let id = next_seq(
            tx,
            make_id_seq_key(project_ns(project_id, self.typ.path().as_bytes()).as_slice()),
        )?;

        let order = next_zero_seq(
            tx,
            make_id_seq_key(
                project_ns(
                    project_id,
                    format!("{}/{}", self.typ.order_path(), req.data_type.short_name()).as_bytes(),
                )
                .as_slice(),
            ),
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

        let idx_key = make_data_value_key(
            project_ns(project_id, self.typ.path().as_bytes()).as_slice(),
            prop.id,
        );
        self.name_cache
            .write()
            .unwrap()
            .put((project_id, prop.name.to_string()), prop.clone());
        self.id_cache
            .write()
            .unwrap()
            .put((project_id, id), prop.clone());

        let data = serialize(&prop)?;
        tx.put(idx_key, &data)?;

        insert_index(tx, idx_keys.as_ref(), &data)?;

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

        match self
            .opti_db
            .add_field(TABLE_EVENTS, prop.column_name().as_str(), dt, req.nullable)
        {
            Ok(_) => Ok(prop),
            Err(StoreError::AlreadyExists(_)) => Ok(prop),
            Err(err) => Err(err.into()),
        }
    }

    pub fn create(&self, project_id: u64, req: CreatePropertyRequest) -> Result<Property> {
        let project_id = if self.typ == Type::System {
            0
        } else {
            project_id
        };

        let tx = self.db.transaction();
        let ret = self.create_(&tx, project_id, req)?;
        tx.commit()?;

        Ok(ret)
    }

    pub fn get_or_create(&self, project_id: u64, req: CreatePropertyRequest) -> Result<Property> {
        let project_id = if self.typ == Type::System {
            0
        } else {
            project_id
        };

        let tx = self.db.transaction();
        match self.get_by_name_(&tx, project_id, req.name.as_str()) {
            Ok(event) => return Ok(event),
            Err(MetadataError::NotFound(_)) => {}
            Err(err) => return Err(err),
        }
        let ret = self.create_(&tx, project_id, req)?;

        tx.commit()?;

        Ok(ret)
    }

    pub fn get_by_id(&self, project_id: u64, id: u64) -> Result<Property> {
        let tx = self.db.transaction();
        self.get_by_id_(&tx, project_id, id)
    }

    pub fn get_by_name(&self, project_id: u64, name: &str) -> Result<Property> {
        let project_id = if self.typ == Type::System {
            0
        } else {
            project_id
        };

        let tx = self.db.transaction();
        self.get_by_name_(&tx, project_id, name)
    }

    pub fn list(&self, project_id: u64) -> Result<ListResponse<Property>> {
        let project_id = if self.typ == Type::System {
            0
        } else {
            project_id
        };

        let tx = self.db.transaction();
        list_data(
            &tx,
            project_ns(project_id, self.typ.path().as_bytes()).as_slice(),
        )
    }

    pub fn update(
        &self,
        project_id: u64,
        property_id: u64,
        req: UpdatePropertyRequest,
    ) -> Result<Property> {
        let project_id = if self.typ == Type::System {
            0
        } else {
            project_id
        };

        let tx = self.db.transaction();

        let prev_prop = self.get_by_id(project_id, property_id)?;
        let mut prop = prev_prop.clone();

        let mut idx_keys: Vec<Option<Vec<u8>>> = Vec::new();
        let mut idx_prev_keys: Vec<Option<Vec<u8>>> = Vec::new();
        // name is persistent
        // if let OptionalProperty::Some(name) = &req.name {
        //     idx_keys.push(index_name_key(
        //
        //         project_id,
        //         &self.typ,
        //         name.as_str(),
        //     ));
        //     idx_prev_keys.push(index_name_key(
        //
        //         project_id,
        //         &self.typ,
        //         prev_prop.name.as_str(),
        //     ));
        //     prop.name = name.to_owned();
        // }
        if let OptionalProperty::Some(display_name) = &req.display_name {
            idx_keys.push(index_display_name_key(
                project_id,
                &self.typ,
                display_name.clone(),
            ));
            idx_prev_keys.push(index_display_name_key(
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

        let idx_key = make_data_value_key(
            project_ns(project_id, self.typ.path().as_bytes()).as_slice(),
            prop.id,
        );
        self.name_cache
            .write()
            .unwrap()
            .put((project_id, prop.name.to_string()), prop.clone());
        self.id_cache
            .write()
            .unwrap()
            .put((project_id, prop.id), prop.clone());

        let data = serialize(&prop)?;
        tx.put(idx_key, &data)?;

        update_index(&tx, idx_keys.as_ref(), idx_prev_keys.as_ref(), &data)?;
        tx.commit()?;
        Ok(prop)
    }

    pub fn delete(&self, project_id: u64, id: u64) -> Result<Property> {
        let project_id = if self.typ == Type::System {
            0
        } else {
            project_id
        };

        let tx = self.db.transaction();
        let prop = self.get_by_id_(&tx, project_id, id)?;
        tx.delete(make_data_value_key(
            project_ns(project_id, self.typ.path().as_bytes()).as_slice(),
            id,
        ))?;

        delete_index(
            &tx,
            index_keys(project_id, &self.typ, &prop.name, prop.display_name.clone()).as_ref(),
        )?;
        self.name_cache
            .write()
            .unwrap()
            .pop(&(project_id, prop.name.to_string()));
        self.id_cache.write().unwrap().pop(&(project_id, prop.id));
        tx.commit()?;
        Ok(prop)
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub enum Status {
    #[default]
    Enabled,
    Disabled,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum Type {
    System,
    Event,
    User,
}

impl Type {
    pub fn path(&self) -> &str {
        match self {
            Type::System => "system_properties",
            Type::Event => "event_properties",
            Type::User => "user_properties",
        }
    }

    pub fn order_path(&self) -> &str {
        match self {
            Type::System => "system_properties/order",
            _ => "properties/order",
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum DictionaryType {
    Int8,
    Int16,
    Int32,
    Int64,
}

impl From<DictionaryType> for datatypes::DataType {
    fn from(value: DictionaryType) -> Self {
        match value {
            DictionaryType::Int8 => datatypes::DataType::Int8,
            DictionaryType::Int16 => datatypes::DataType::Int16,
            DictionaryType::Int32 => datatypes::DataType::Int32,
            DictionaryType::Int64 => datatypes::DataType::Int64,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Property {
    pub id: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
    pub created_by: u64,
    pub updated_by: Option<u64>,
    pub project_id: u64,
    pub tags: Option<Vec<String>>,
    pub name: String,
    pub description: Option<String>,
    pub display_name: Option<String>,
    pub order: u64,
    pub typ: Type,
    pub data_type: DType,
    pub status: Status,
    pub is_system: bool,
    pub nullable: bool,
    // this also defines whether property is required or not
    pub is_array: bool,
    pub is_dictionary: bool,
    pub dictionary_type: Option<DictionaryType>,
}

impl Property {
    pub fn column_name(&self) -> String {
        match self.typ {
            Type::System => {
                let mut name: String = self
                    .name
                    .chars()
                    .filter(|c| {
                        c.is_ascii_alphabetic() || c.is_numeric() || c.is_whitespace() || c == &'_'
                    })
                    .collect();
                name = name.to_case(Case::Snake);
                name = name.trim().to_string();

                name
            }
            _ => {
                format!("{}_{}", self.data_type.short_name(), self.order)
            }
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct CreatePropertyRequest {
    pub created_by: u64,
    pub tags: Option<Vec<String>>,
    pub name: String,
    pub description: Option<String>,
    pub display_name: Option<String>,
    pub typ: Type,
    pub data_type: DType,
    pub status: Status,
    pub is_system: bool,
    pub nullable: bool,
    pub is_array: bool,
    pub is_dictionary: bool,
    pub dictionary_type: Option<DictionaryType>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct UpdatePropertyRequest {
    pub updated_by: u64,
    pub tags: OptionalProperty<Option<Vec<String>>>,
    pub name: OptionalProperty<String>,
    pub description: OptionalProperty<Option<String>>,
    pub display_name: OptionalProperty<Option<String>>,
    pub typ: OptionalProperty<Type>,
    pub data_type: OptionalProperty<DType>,
    pub status: OptionalProperty<Status>,
    pub is_system: OptionalProperty<bool>,
    pub nullable: OptionalProperty<bool>,
    pub is_array: OptionalProperty<bool>,
    pub is_dictionary: OptionalProperty<bool>,
    pub dictionary_type: OptionalProperty<Option<DictionaryType>>,
}
