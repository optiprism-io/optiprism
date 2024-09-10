use std::str::from_utf8;
use std::str::pattern::Pattern;
use std::sync::Arc;

use common::GROUPS_COUNT;
use prost::Message;
use rocksdb::Transaction;
use rocksdb::TransactionDB;
use serde::Deserialize;
use serde::Serialize;

use crate::error::MetadataError;
use crate::group;
use crate::index::next_seq;
use crate::index::next_zero_seq;
use crate::make_id_seq_key;
use crate::metadata::ListResponse;
use crate::metadata::ResponseMetadata;
use crate::project_ns;
use crate::Result;

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
        match tx.get(key.as_bytes())? {
            None => {
                let group_key = format!("groups/{group_id}");
                let id = next_seq(
                    tx,
                    make_id_seq_key(project_ns(project_id, group_key.as_bytes()).as_slice()),
                )?;
                tx.put(key.as_bytes(), id.to_le_bytes())?;

                Ok(id)
            }
            Some(key) => Ok(u64::from_le_bytes(key.try_into().unwrap())),
        }
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
    ) -> Result<GroupValues> {
        let tx = self.db.transaction();
        let user_key = format!("projects/{project_id}/groups/{group_id}/real/keys/{id}");
        let res = match tx.get(user_key.as_bytes())? {
            None => {
                let anonymous_id =
                    self.get_or_create_anonymous_id(project_id, group_id, anonymous)?;
                let group_key = format!("groups/{group_id}");
                let seq_key =
                    make_id_seq_key(project_ns(project_id, group_key.as_bytes()).as_slice());
                tx.put(seq_key, anonymous_id.to_string().as_bytes())?;
                let group = GroupValues {
                    id: anonymous_id,
                    values,
                };
                tx.put(user_key, serialize_group_values(&group)?)?;

                group
            }
            Some(value) => deserialize_group_values(&value)?,
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
    ) -> Result<GroupValues> {
        let key = format!("projects/{project_id}/groups/{group_id}/real/keys/{key}");
        match tx.get(key.as_bytes())? {
            None => {
                let group_key = format!("groups/{group_id}");
                let id = next_seq(
                    tx,
                    make_id_seq_key(project_ns(project_id, group_key.as_bytes()).as_slice()),
                )?;
                let group = GroupValues { id, values };
                tx.put(key.as_bytes(), serialize_group_values(&group)?)?;

                Ok(group)
            }
            Some(value) => Ok(deserialize_group_values(&value)?),
        }
    }

    // get anonymous id by key or create one
    pub fn get_or_create(
        &self,
        project_id: u64,
        group_id: u64,
        key: &str,
        values: Vec<PropertyValue>,
    ) -> Result<GroupValues> {
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
    ) -> Result<GroupValues> {
        let tx = self.db.transaction();
        let key = format!("projects/{project_id}/groups/{group_id}/real/keys/{key}");
        let group = match tx.get(key.as_bytes())? {
            None => {
                let group_key = format!("groups/{group_id}");
                let id = next_seq(
                    &tx,
                    make_id_seq_key(project_ns(project_id, group_key.as_bytes()).as_slice()),
                )?;
                let group = GroupValues { id, values };
                tx.put(key.as_bytes(), serialize_group_values(&group)?)?;
                group
            }
            Some(value) => {
                let group: GroupValues = deserialize_group_values(&value)?;
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
                let group = GroupValues {
                    id: group.id,
                    values: vals,
                };
                tx.put(key.as_bytes(), serialize_group_values(&group)?)?;

                group
            }
        };
        tx.commit()?;
        Ok(group)
    }

    pub fn list_groups(&self, project_id: u64) -> Result<ListResponse<Group>> {
        let tx = self.db.transaction();
        let prefix = format!("projects/{project_id}/groups/names");
        let iter = tx.prefix_iterator(prefix.clone());
        let mut list = vec![];
        for kv in iter {
            let (key, value) = kv?;
            // check if key contains the prefix
            if !prefix.is_prefix_of(from_utf8(&key).unwrap()) {
                break;
            }
            list.push(deserialize_group(&value)?);
        }

        Ok(ListResponse {
            data: list,
            meta: ResponseMetadata { next: None },
        })
    }

    pub fn get_or_create_group(
        &self,
        project_id: u64,
        name: String,
        display_name: String,
    ) -> Result<Group> {
        let tx = self.db.transaction();
        let key = format!("projects/{project_id}/groups/names/{name}");
        let group = match tx.get(key.as_bytes())? {
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
                let group = Group {
                    id,
                    name,
                    display_name,
                };
                tx.put(key.as_bytes(), serialize_group(&group)?)?;

                group
            }
            Some(value) => deserialize_group(&value)?,
        };

        tx.commit()?;

        Ok(group)
    }

    pub fn next_record_sequence(&self, project_id: u64, group_id: u64) -> Result<u64> {
        let group_key = format!("groups/{group_id}/records/");
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
pub struct GroupValues {
    pub id: u64,
    pub values: Vec<PropertyValue>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub struct Group {
    pub id: u64,
    pub name: String,
    pub display_name: String,
}

fn serialize_group_values(group: &GroupValues) -> Result<Vec<u8>> {
    let v = group::GroupValues {
        id: group.id,
        values: group
            .values
            .iter()
            .map(|v| group::PropertyValue {
                property_id: v.property_id,
                value: Some(match &v.value {
                    Value::Null => group::Value {
                        value: Some(group::value::Value::NullValue(true)),
                    },
                    Value::Int8(v) => group::Value {
                        value: Some(group::value::Value::Int8Value(v.unwrap() as i64)),
                    },
                    Value::Int16(v) => group::Value {
                        value: Some(group::value::Value::Int16Value(v.unwrap() as i64)),
                    },
                    Value::Int32(v) => group::Value {
                        value: Some(group::value::Value::Int32Value(v.unwrap() as i64)),
                    },
                    Value::Int64(v) => group::Value {
                        value: Some(group::value::Value::Int64Value(v.unwrap())),
                    },
                    Value::Boolean(v) => group::Value {
                        value: Some(group::value::Value::BoolValue(v.unwrap())),
                    },
                    Value::Timestamp(v) => group::Value {
                        value: Some(group::value::Value::TimestampValue(v.unwrap())),
                    },
                    Value::Decimal(v) => group::Value {
                        value: Some(group::value::Value::DecimalValue(
                            v.unwrap().to_le_bytes().to_vec(),
                        )),
                    },
                    Value::String(v) => group::Value {
                        value: Some(group::value::Value::StringValue(v.clone().unwrap())),
                    },
                    _ => unimplemented!(),
                }),
            })
            .collect::<Vec<_>>(),
    };

    Ok(v.encode_to_vec())
}

fn deserialize_group_values(data: &[u8]) -> Result<GroupValues> {
    let from = group::GroupValues::decode(data)?;

    Ok(GroupValues {
        id: from.id,
        values: from
            .values
            .iter()
            .map(|v| {
                let value = match v.value.clone().unwrap().value.unwrap() {
                    group::value::Value::NullValue(_) => Value::Null,
                    group::value::Value::Int8Value(v) => Value::Int8(Some(v as i8)),
                    group::value::Value::Int16Value(v) => Value::Int16(Some(v as i16)),
                    group::value::Value::Int32Value(v) => Value::Int32(Some(v as i32)),
                    group::value::Value::Int64Value(v) => Value::Int64(Some(v)),
                    group::value::Value::BoolValue(v) => Value::Boolean(Some(v)),
                    group::value::Value::TimestampValue(v) => Value::Timestamp(Some(v)),
                    group::value::Value::DecimalValue(v) => {
                        Value::Decimal(Some(i128::from_le_bytes(v.as_slice().try_into().unwrap())))
                    }
                    group::value::Value::StringValue(v) => Value::String(Some(v.clone())),
                };

                PropertyValue {
                    property_id: v.property_id,
                    value,
                }
            })
            .collect::<Vec<_>>(),
    })
}

// serialize group into protobuf
fn serialize_group(group: &Group) -> Result<Vec<u8>> {
    let v = group::Group {
        id: group.id,
        name: group.name.clone(),
        display_name: Some(group.display_name.clone()),
    };

    Ok(v.encode_to_vec())
}

// deserialize group from protobuf
fn deserialize_group(data: &[u8]) -> Result<Group> {
    let from = group::Group::decode(data)?;

    Ok(Group {
        id: from.id,
        name: from.name.clone(),
        display_name: from.display_name.unwrap(),
    })
}
#[cfg(test)]
mod tests {
    #[test]
    fn test_group_values_roundtrip() {
        let group = super::GroupValues {
            id: 1,
            values: vec![
                super::PropertyValue {
                    property_id: 1,
                    value: super::Value::Int64(Some(1)),
                },
                super::PropertyValue {
                    property_id: 2,
                    value: super::Value::String(Some("test".to_string())),
                },
            ],
        };

        let data = super::serialize_group_values(&group).unwrap();
        let group2 = super::deserialize_group_values(&data).unwrap();

        assert_eq!(group, group2);
    }

    #[test]
    fn test_group_roundtrip() {
        let group = super::Group {
            id: 1,
            name: "test".to_string(),
            display_name: "Test".to_string(),
        };

        let data = super::serialize_group(&group).unwrap();
        let group2 = super::deserialize_group(&data).unwrap();

        assert_eq!(group, group2);
    }
}
