use std::fmt::Debug;
use std::fmt::Formatter;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;

use byteorder::ByteOrder;
use byteorder::LittleEndian;
use rocksdb::TransactionDB;

use crate::error::MetadataError;
use crate::error::Result;
use crate::index::next_seq;
use crate::make_id_seq_key;
use crate::project_ns;

const NAMESPACE: &[u8] = b"dictinaries";

fn dict_ns(dict: &str) -> Vec<u8> {
    [NAMESPACE, b"/", dict.as_bytes()].concat()
}

fn make_key_key(project_id: u64, dict: &str, key: u64) -> Vec<u8> {
    [
        project_ns(project_id, dict_ns(dict).as_slice()).as_slice(),
        b"keys/",
        key.to_le_bytes().as_ref(),
    ]
    .concat()
}

fn make_value_key(project_id: u64, dict: &str, value: &str) -> Vec<u8> {
    [
        project_ns(project_id, dict_ns(dict).as_slice()).as_slice(),
        b"values/",
        value.as_bytes(),
    ]
    .concat()
}

pub struct Dictionaries {
    db: Arc<TransactionDB>,
}

impl Dictionaries {
    pub fn new(db: Arc<TransactionDB>) -> Self {
        Self { db }
    }
}

impl Debug for Dictionaries {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "dictionaries")
    }
}

impl Dictionaries {
    pub fn get_key_or_create(&self, project_id: u64, dict: &str, value: &str) -> Result<u64> {
        let tx = self.db.transaction();
        let res = match tx.get(make_value_key(project_id, dict, value))? {
            None => {
                let id = next_seq(
                    &tx,
                    make_id_seq_key(project_ns(project_id, dict_ns(dict).as_slice()).as_slice()),
                )?;
                tx.put(make_key_key(project_id, dict, id), value.as_bytes())?;
                tx.put(
                    make_value_key(project_id, dict, value),
                    id.to_le_bytes().as_ref(),
                )?;

                Ok(id)
            }
            Some(key) => Ok(LittleEndian::read_u64(key.as_slice())),
        };
        tx.commit()?;
        res
    }

    pub fn get_value(&self, project_id: u64, dict: &str, key: u64) -> Result<String> {
        let tx = self.db.transaction();
        let store_key = make_key_key(project_id, dict, key);
        match tx.get(store_key.as_slice())? {
            None => Err(MetadataError::NotFound(format!(
                "project {project_id}, dict {dict}, value for key {key} not found"
            ))),
            Some(value) => Ok(String::from_utf8(value)?),
        }
    }

    pub fn get_key(&self, project_id: u64, dict: &str, value: &str) -> Result<u64> {
        let tx = self.db.transaction();
        let store_key = make_value_key(project_id, dict, value);
        match tx.get(store_key.as_slice())? {
            None => Err(MetadataError::NotFound(format!(
                "dictionary key by value {} not found",
                value
            ))),
            Some(key) => Ok(LittleEndian::read_u64(key.as_slice())),
        }
    }
}

#[derive(Debug)]
pub struct SingleDictionaryProvider {
    project_id: u64,
    dict: String,
    dictionaries: Arc<Dictionaries>,
}

impl Hash for SingleDictionaryProvider {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.project_id.hash(state);
        self.dict.hash(state);
    }
}

impl SingleDictionaryProvider {
    pub fn new(project_id: u64, dict: String, dictionaries: Arc<Dictionaries>) -> Self {
        Self {
            project_id,
            dict,
            dictionaries,
        }
    }

    pub fn get_key_or_create(&self, value: &str) -> Result<u64> {
        self.dictionaries
            .get_key_or_create(self.project_id, self.dict.as_str(), value)
    }

    pub fn get_value(&self, key: u64) -> Result<String> {
        self.dictionaries
            .get_value(self.project_id, self.dict.as_str(), key)
    }

    pub fn get_key(&self, _project_id: u64, _dict: &str, value: &str) -> Result<u64> {
        self.dictionaries
            .get_key(self.project_id, self.dict.as_str(), value)
    }
}
