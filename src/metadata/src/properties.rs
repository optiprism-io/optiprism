use std::num::NonZeroUsize;
use std::str::from_utf8;
use std::str::pattern::Pattern;
use std::sync::Arc;
use std::sync::RwLock;

use arrow::datatypes;
use arrow::datatypes::DataType;
use chrono::DateTime;
use chrono::Utc;
use common::group_col;
use common::query::PropertyRef;
use common::types::DType;
use common::types::OptionalProperty;
use common::types::TABLE_EVENTS;
use common::GROUPS_COUNT;
use convert_case::Case;
use convert_case::Casing;
use lru::LruCache;
use prost::Message;
use rocksdb::Transaction;
use rocksdb::TransactionDB;
use serde::Deserialize;
use serde::Serialize;
use storage::db::OptiDBImpl;
use storage::error::StoreError;

use crate::error::MetadataError;
use crate::index::check_update_constraints;
use crate::index::delete_index;
use crate::index::get_index;
use crate::index::insert_index;
use crate::index::next_seq;
use crate::index::next_zero_seq;
use crate::index::update_index;
use crate::make_data_key;
use crate::make_data_value_key;
use crate::make_id_seq_key;
use crate::make_index_key;
use crate::metadata::ListResponse;
use crate::metadata::ResponseMetadata;
use crate::project_ns;
use crate::property;
use crate::Result;

const IDX_NAME: &[u8] = b"name";
const IDX_COLUMN_NAME: &[u8] = b"column_name";
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
    pub fn new(db: Arc<TransactionDB>, opti_db: Arc<OptiDBImpl>) -> Self {
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
    pub fn new_group(db: Arc<TransactionDB>, opti_db: Arc<OptiDBImpl>) -> Vec<Arc<Self>> {
        (0..GROUPS_COUNT)
            .map(|gid| {
                let id_cache = RwLock::new(LruCache::new(
                    NonZeroUsize::new(10 /* todo why 10? */).unwrap(),
                ));
                let name_cache = RwLock::new(LruCache::new(
                    NonZeroUsize::new(10 /* todo why 10? */).unwrap(),
                ));
                Arc::new(Properties {
                    db: db.clone(),
                    opti_db: opti_db.clone(),
                    id_cache,
                    name_cache,
                    typ: Type::Group(gid),
                })
            })
            .collect::<Vec<_>>()
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

    pub fn get_by_column_name_global(&self, project_id: u64, name: &str) -> Result<Property> {
        let tx = self.db.transaction();
        let idx_key = make_index_key(
            project_ns(project_id, self.typ.path().as_bytes()).as_slice(),
            IDX_NAME,
            name,
        );
        let id = get_index(
            &tx,
            idx_key,
            format!("property with col name \"{}\" not found", name).as_str(),
        )?;

        self.get_by_id_(&tx, project_id, id)
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
        let id = get_index(
            tx,
            idx_key,
            format!("property with name \"{}\" not found", name).as_str(),
        )?;

        self.get_by_id_(tx, project_id, id)
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

        // todo писать в кеш

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
        let mut idx_keys = index_keys(project_id, &self.typ, &req.name, req.display_name.clone());

        for key in idx_keys.iter().flatten() {
            if tx.get(key)?.is_some() {
                return Err(MetadataError::AlreadyExists(String::from_utf8(
                    key.to_owned(),
                )?));
            }
        }

        let id = next_seq(
            tx,
            make_id_seq_key(project_ns(project_id, self.typ.path().as_bytes()).as_slice()),
        )?;

        let order = next_zero_seq(
            tx,
            make_id_seq_key(
                project_ns(
                    project_id,
                    format!("properties/order/{}", req.data_type.short_name()).as_bytes(),
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
            typ: req.typ.clone(),
            data_type: req.data_type.clone(),
            status: req.status,
            hidden: req.hidden,
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
        tx.put(idx_key, data)?;

        idx_keys.push(Some(
            make_index_key(
                project_ns(project_id, "global".as_bytes()).as_slice(),
                IDX_COLUMN_NAME,
                prop.column_name().as_str(),
            )
            .to_vec(),
        ));

        insert_index(tx, idx_keys.as_ref(), prop.id)?;

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

        return if let Type::Group(g) = self.typ {
            match self.opti_db.add_field(
                TABLE_EVENTS,
                prop.column_name().as_str(),
                dt.clone(),
                req.nullable,
            ) {
                Err(StoreError::AlreadyExists(_)) => {}
                Err(err) => return Err(err.into()),
                _ => {}
            };

            match self.opti_db.add_field(
                group_col(g).as_str(),
                prop.column_name().as_str(),
                dt,
                req.nullable,
            ) {
                Ok(_) => Ok(prop),
                Err(StoreError::AlreadyExists(_)) => Ok(prop),
                Err(err) => Err(err.into()),
            }
        } else {
            match self.opti_db.add_field(
                TABLE_EVENTS,
                prop.column_name().as_str(),
                dt,
                req.nullable,
            ) {
                Ok(_) => Ok(prop),
                Err(StoreError::AlreadyExists(_)) => Ok(prop),
                Err(err) => Err(err.into()),
            }
        };
    }

    pub fn create(&self, project_id: u64, req: CreatePropertyRequest) -> Result<Property> {
        let tx = self.db.transaction();
        let ret = self.create_(&tx, project_id, req)?;
        tx.commit()?;

        Ok(ret)
    }

    pub fn get_or_create(&self, project_id: u64, req: CreatePropertyRequest) -> Result<Property> {
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
        let tx = self.db.transaction();
        self.get_by_name_(&tx, project_id, name)
    }

    pub fn get_by_column_name(&self, project_id: u64, col_name: &str) -> Result<Property> {
        let props = self.list(project_id)?;
        for prop in props.data {
            if prop.column_name() == col_name {
                return Ok(prop);
            }
        }

        Err(MetadataError::NotFound(
            format!("property with column name \"{}\" not found", col_name).to_string(),
        ))
    }
    pub fn list(&self, project_id: u64) -> Result<ListResponse<Property>> {
        let tx = self.db.transaction();

        let prefix = make_data_key(project_ns(project_id, self.typ.path().as_bytes()).as_slice());

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
        property_id: u64,
        req: UpdatePropertyRequest,
    ) -> Result<Property> {
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
            display_name.clone_into(&mut prop.display_name);
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
        tx.put(idx_key, data)?;

        update_index(&tx, idx_keys.as_ref(), idx_prev_keys.as_ref(), property_id)?;
        tx.commit()?;
        Ok(prop)
    }

    pub fn delete(&self, project_id: u64, id: u64) -> Result<Property> {
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

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub enum Type {
    #[default]
    Event,
    Group(usize),
}

impl Type {
    pub fn path(&self) -> String {
        match self {
            Type::Event => "event_properties".to_string(),
            Type::Group(gid) => {
                format!("group_{gid}_properties")
            }
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
    pub hidden: bool,
    // this also defines whether property is required or not
    pub is_array: bool,
    pub is_dictionary: bool,
    pub dictionary_type: Option<DictionaryType>,
}

impl Property {
    pub fn column_name(&self) -> String {
        if self.is_system {
            let mut name: String = self
                .name
                .chars()
                .filter(|c| {
                    c.is_ascii_alphabetic() || c.is_numeric() || c.is_whitespace() || c == &'_'
                })
                .collect();
            name = name.to_case(Case::Snake);
            name = name.trim().to_string();

            return name;
        }

        match self.typ {
            Type::Event => format!("e_{}_{}", self.data_type.short_name(), self.order),
            Type::Group(gid) => {
                format!("g_{gid}_{}_{}", self.data_type.short_name(), self.order)
            }
        }
    }

    pub fn name(&self) -> String {
        match &self.display_name {
            None => self.name.to_owned(),
            Some(dn) => dn.to_owned(),
        }
    }

    pub fn data_type(&self) -> DataType {
        if let Some(dt) = &self.dictionary_type {
            dt.to_owned().into()
        } else {
            self.data_type.to_owned().into()
        }
    }

    pub fn reference(&self) -> PropertyRef {
        match self.typ {
            Type::Event => PropertyRef::Event(self.name.to_owned()),
            Type::Group(v) => PropertyRef::Group(self.name.to_owned(), v),
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
    pub hidden: bool,
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

// serialize property to protobuf
fn serialize(prop: &Property) -> Result<Vec<u8>> {
    let mut v = property::Property {
        id: prop.id,
        created_at: prop.created_at.timestamp(),
        created_by: prop.created_by,
        updated_at: prop.updated_at.map(|t| t.timestamp()),
        updated_by: prop.updated_by,
        project_id: prop.project_id,
        tags: prop.tags.clone().unwrap_or_default(),
        name: prop.name.clone(),
        description: prop.description.clone(),
        display_name: prop.display_name.clone(),
        order: prop.order,
        type_event: None,
        type_group: None,
        dtype: match &prop.data_type {
            DType::Int8 => property::DataType::Int8 as i32,
            DType::Int16 => property::DataType::Int16 as i32,
            DType::Int32 => property::DataType::Int32 as i32,
            DType::Int64 => property::DataType::Int64 as i32,
            DType::Boolean => property::DataType::Boolean as i32,
            DType::Timestamp => property::DataType::Timestamp as i32,
            DType::Decimal => property::DataType::Decimal as i32,
            DType::String => property::DataType::String as i32,
            _ => unreachable!(),
        },
        status: match &prop.status {
            Status::Enabled => property::Status::Enabled as i32,
            Status::Disabled => property::Status::Disabled as i32,
        },
        is_system: prop.is_system,
        nullable: prop.nullable,
        hidden: prop.hidden,
        is_array: prop.is_array,
        is_dictionary: prop.is_dictionary,
        dictionary_type: prop.dictionary_type.clone().map(|v| match v {
            DictionaryType::Int8 => property::DictionaryType::DictionaryInt8 as i32,
            DictionaryType::Int16 => property::DictionaryType::DictionaryInt16 as i32,
            DictionaryType::Int32 => property::DictionaryType::DictionaryInt32 as i32,
            DictionaryType::Int64 => property::DictionaryType::DictionaryInt64 as i32,
        }),
    };
    match prop.typ {
        Type::Event => v.type_event = Some(true),
        Type::Group(g) => v.type_group = Some(g as u64),
    }

    Ok(v.encode_to_vec())
}

// deserialize property from protobuf
fn deserialize(data: &[u8]) -> Result<Property> {
    let from = property::Property::decode(data)?;

    Ok(Property {
        id: from.id,
        created_at: chrono::DateTime::from_timestamp(from.created_at, 0).unwrap(),
        created_by: from.created_by,
        updated_at: from
            .updated_at
            .map(|t| chrono::DateTime::from_timestamp(t, 0).unwrap()),
        updated_by: from.updated_by,
        project_id: from.project_id,
        tags: if from.tags.is_empty() {
            None
        } else {
            Some(from.tags)
        },
        name: from.name,
        description: from.description,
        display_name: from.display_name,
        order: from.order,
        typ: if from.type_event.unwrap_or(false) {
            Type::Event
        } else {
            Type::Group(from.type_group.unwrap() as usize)
        },
        data_type: match from.dtype {
            1 => DType::String,
            2 => DType::Int8,
            3 => DType::Int16,
            4 => DType::Int32,
            5 => DType::Int64,
            6 => DType::Decimal,
            7 => DType::Boolean,
            8 => DType::Timestamp,
            _ => unreachable!(),
        },
        status: match from.status {
            1 => Status::Enabled,
            2 => Status::Disabled,
            _ => unreachable!(),
        },
        is_system: from.is_system,
        nullable: from.nullable,
        hidden: from.hidden,
        is_array: from.is_array,
        is_dictionary: from.is_dictionary,
        dictionary_type: from.dictionary_type.map(|v| match v {
            1 => DictionaryType::Int8,
            2 => DictionaryType::Int16,
            3 => DictionaryType::Int32,
            4 => DictionaryType::Int64,
            _ => unreachable!(),
        }),
    })
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;
    use common::types::DType;

    use crate::properties::deserialize;
    use crate::properties::serialize;
    use crate::properties::DictionaryType;
    use crate::properties::Property;
    use crate::properties::Status;
    use crate::properties::Type;

    #[test]
    fn test_roundtrip() {
        let prop = Property {
            id: 1,
            created_at: DateTime::from_timestamp(1, 0).unwrap(),
            created_by: 1,
            updated_at: Some(DateTime::from_timestamp(2, 0).unwrap()),
            updated_by: Some(2),
            project_id: 1,
            tags: Some(vec!["tag1".to_string(), "tag2".to_string()]),
            name: "name".to_string(),
            description: Some("description".to_string()),
            display_name: Some("display_name".to_string()),
            order: 1,
            typ: Type::Event,
            data_type: DType::Int32,
            status: Status::Enabled,
            is_system: true,
            nullable: true,
            hidden: true,
            is_array: true,
            is_dictionary: true,
            dictionary_type: Some(DictionaryType::Int16),
        };

        let data = serialize(&prop).unwrap();
        let prop2 = deserialize(&data).unwrap();

        assert_eq!(prop, prop2);
    }
}
