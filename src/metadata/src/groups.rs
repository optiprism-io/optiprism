use std::sync::Arc;

use bincode::deserialize;
use bincode::serialize;
use byteorder::ByteOrder;
use byteorder::LittleEndian;
use common::GROUPS_COUNT;
use rocksdb::Transaction;
use rocksdb::TransactionDB;
use serde::Deserialize;
use serde::Serialize;

use crate::error::MetadataError;
use crate::events::Event;
use crate::index::next_seq;
use crate::index::next_zero_seq;
use crate::list_data;
use crate::make_data_value_key;
use crate::make_id_seq_key;
use crate::metadata::ListResponse;
use crate::metadata::ResponseMetadata;
use crate::project_ns;
use crate::Result;

const NAMESPACE: &[u8] = b"groups";
const REAL_NAMESPACE: &[u8] = b"groups/real";
const ANONYMOUS_NAMESPACE: &[u8] = b"groups/anonymous";

pub struct Groups {
    db: Arc<TransactionDB>,
}

impl Groups {
    pub fn new(db: Arc<TransactionDB>) -> Self {
        Groups { db }
    }

    fn get_or_create_anonymous_id_(
        &self,
        tx: &Transaction<TransactionDB>,
        project_id: u64,
        group_id: u64,
        anon_key: &str,
    ) -> Result<u64> {
        let key = format!("projects/{project_id}/groups/{group_id}/anonymous/keys/{anon_key}");
        let res = match tx.get(key.as_bytes())? {
            None => {
                let group_key = format!("groups/{group_id}");
                let id = next_seq(
                    &tx,
                    make_id_seq_key(project_ns(project_id, group_key.as_bytes()).as_slice()),
                )?;
                tx.put(key.as_bytes(), id.to_le_bytes().as_ref())?;

                Ok(id)
            }
            Some(key) => Ok(LittleEndian::read_u64(key.as_slice())),
        };
        res
    }

    // get anonymous id by key or create one
    pub fn get_or_create_anonymous_id(
        &self,
        project_id: u64,
        group_id: u64,
        key: &str,
    ) -> Result<u64> {
        let tx = self.db.transaction();
        let id = self.get_or_create_anonymous_id_(&tx, project_id, group_id, key)?;
        tx.commit()?;
        Ok(id)
    }

    // try to find user id by user key
    // if not found, try to get or create anonymous key and assign id to user
    // return user group
    pub fn merge_with_anonymous(
        &self,
        project_id: u64,
        group_id: u64,
        anonymous: &str,
        id: &str,
        values: Vec<PropertyValue>,
    ) -> Result<Group> {
        let tx = self.db.transaction();
        let user_key = format!("projects/{project_id}/groups/{group_id}/real/keys/{id}");
        let res = match tx.get(user_key.as_bytes())? {
            None => {
                let anonymous_id =
                    self.get_or_create_anonymous_id(project_id, group_id, anonymous)?;
                let group_key = format!("groups/{group_id}");
                let seq_key =
                    make_id_seq_key(project_ns(project_id, group_key.as_bytes()).as_slice());
                tx.put(&seq_key, anonymous_id.to_le_bytes().as_ref())?;
                let group = Group {
                    id: anonymous_id,
                    values,
                };
                tx.put(user_key, serialize(&group)?)?;

                group
            }
            Some(value) => deserialize(&value)?,
        };

        tx.commit()?;
        Ok(res)
    }

    fn get_or_create_(
        &self,
        tx: &Transaction<TransactionDB>,
        project_id: u64,
        group_id: u64,
        key: &str,
        values: Vec<PropertyValue>,
    ) -> Result<Group> {
        let key = format!("projects/{project_id}/groups/{group_id}/real/keys/{key}");
        let res = match tx.get(key.as_bytes())? {
            None => {
                let group_key = format!("groups/{group_id}");
                let id = next_seq(
                    &tx,
                    make_id_seq_key(project_ns(project_id, group_key.as_bytes()).as_slice()),
                )?;

                let group = Group { id, values };
                tx.put(key.as_bytes(), serialize(&group)?)?;

                group
            }
            Some(value) => deserialize(&value)?,
        };

        Ok(res)
    }

    // get anonymous id by key or create one
    pub fn get_or_create(
        &self,
        project_id: u64,
        group_id: u64,
        key: &str,
        values: Vec<PropertyValue>,
    ) -> Result<Group> {
        let tx = self.db.transaction();
        let id = self.get_or_create_(&tx, project_id, group_id, key, values)?;
        tx.commit()?;
        Ok(id)
    }

    pub fn create_or_update(
        &self,
        project_id: u64,
        group_id: u64,
        key: &str,
        values: Vec<PropertyValue>,
    ) -> Result<Group> {
        let tx = self.db.transaction();
        let key = format!("projects/{project_id}/groups/{group_id}/real/keys/{key}");
        let group = match tx.get(key.as_bytes())? {
            None => {
                let group_key = format!("groups/{group_id}");
                let id = next_seq(
                    &tx,
                    make_id_seq_key(project_ns(project_id, group_key.as_bytes()).as_slice()),
                )?;

                let group = Group { id, values };
                tx.put(key.as_bytes(), serialize(&group)?)?;
                group
            }
            Some(value) => {
                let group: Group = deserialize(&value)?;
                let mut vals = group.values.clone();
                for value in values {
                    let mut found = false;
                    for (gid, gvalue) in group.values.iter().enumerate() {
                        if gvalue.property_id == value.property_id {
                            vals[gid] = value.clone();
                            found = true;
                            break;
                        }
                    }
                    if !found {
                        vals.push(value);
                    }
                }
                let group = Group {
                    id: group.id,
                    values: vals,
                };
                tx.put(key.as_bytes(), serialize(&group)?)?;

                group
            }
        };
        tx.commit()?;
        Ok(group)
    }

    pub fn list_names(&self, project_id: u64) -> Result<ListResponse<(u64, String)>> {
        let tx = self.db.transaction();
        let mut res = vec![];
        let key = format!("projects/{project_id}/groups/names");
        for i in tx.prefix_iterator(key) {
            let i = i?;
            let k = u64::from_le_bytes(i.1.as_ref().try_into().unwrap());
            let v = String::from_utf8(i.0.to_vec())?
                .split("/")
                .last()
                .unwrap()
                .to_string();
            res.push((k, v));
        }

        Ok(ListResponse {
            data: res,
            meta: ResponseMetadata { next: None },
        })
    }

    pub fn get_or_create_group_name(&self, project_id: u64, name: &str) -> Result<u64> {
        let tx = self.db.transaction();
        let key = format!("projects/{project_id}/groups/names/{name}");
        let id = match tx.get(key.as_bytes())? {
            None => {
                let id = next_zero_seq(
                    &tx,
                    make_id_seq_key(project_ns(project_id, "groups".as_bytes()).as_slice()),
                )?;

                if id >= GROUPS_COUNT as u64 {
                    return Err(MetadataError::BadRequest(
                        "group name limit reached".to_string(),
                    ));
                }
                tx.put(key.as_bytes(), id.to_le_bytes().as_ref())?;

                id
            }
            Some(value) => u64::from_le_bytes(value.try_into().unwrap()),
        };

        tx.commit()?;

        Ok(id)
    }

    pub fn next_record_sequence(&self, project_id: u64, group_id: u64) -> Result<u64> {
        let group_key = format!("groups/{group_id}/records");
        let tx = self.db.transaction();

        let id = next_seq(
            &tx,
            make_id_seq_key(project_ns(project_id, group_key.as_bytes()).as_slice()),
        )?;

        tx.commit()?;
        Ok(id)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub enum Value {
    Null,
    Int8(Option<i8>),
    Int16(Option<i16>),
    Int32(Option<i32>),
    Int64(Option<i64>),
    Boolean(Option<bool>),
    Timestamp(Option<i64>),
    Decimal(Option<i128>),
    String(Option<String>),
    ListInt8(Option<Vec<Option<i8>>>),
    ListInt16(Option<Vec<Option<i16>>>),
    ListInt32(Option<Vec<Option<i32>>>),
    ListInt64(Option<Vec<Option<i64>>>),
    ListBoolean(Option<Vec<Option<bool>>>),
    ListTimestamp(Option<Vec<Option<i64>>>),
    ListDecimal(Option<Vec<Option<i128>>>),
    ListString(Option<Vec<Option<String>>>),
}

#[derive(Clone, Debug, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub struct PropertyValue {
    pub property_id: u64,
    pub value: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub struct Group {
    pub id: u64,
    pub values: Vec<PropertyValue>,
}
