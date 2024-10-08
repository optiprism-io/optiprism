use std::fmt::Debug;
use std::fmt::Formatter;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;

use rocksdb::Transaction;
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
        key.to_string().as_bytes(),
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

fn tbl_dict(tbl: &str, dict: &str) -> String {
    format!("tables/{tbl}/{dict}/")
}

impl Dictionaries {
    pub fn _get_key_or_create(
        &self,
        tx: &Transaction<TransactionDB>,
        project_id: u64,
        table: &str,
        dict: &str,
        value: &str,
    ) -> Result<u64> {
        let res = match tx.get(make_value_key(
            project_id,
            tbl_dict(table, dict).as_str(),
            value,
        ))? {
            None => {
                let key = make_id_seq_key(
                    project_ns(
                        project_id,
                        dict_ns(tbl_dict(table, dict).as_str()).as_slice(),
                    )
                    .as_slice(),
                );
                let id = next_seq(tx, key)?;
                tx.put(
                    make_key_key(project_id, tbl_dict(table, dict).as_str(), id),
                    value.as_bytes(),
                )?;
                tx.put(
                    make_value_key(project_id, tbl_dict(table, dict).as_str(), value),
                    id.to_le_bytes(),
                )?;

                Ok(id)
            }
            Some(key) => Ok(u64::from_le_bytes(key.try_into().unwrap())),
        };

        res
    }

    pub fn create_key(
        &self,
        project_id: u64,
        table: &str,
        dict: &str,
        key: u64,
        value: &str,
    ) -> Result<()> {
        let tx = self.db.transaction();
        tx.put(
            make_key_key(project_id, tbl_dict(table, dict).as_str(), key),
            value.as_bytes(),
        )?;
        tx.put(
            make_value_key(project_id, tbl_dict(table, dict).as_str(), value),
            key.to_le_bytes(),
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn get_key_or_create(
        &self,
        project_id: u64,
        table: &str,
        dict: &str,
        value: &str,
    ) -> Result<u64> {
        let tx = self.db.transaction();
        let key = self._get_key_or_create(&tx, project_id, table, dict, value)?;
        tx.commit()?;
        Ok(key)
    }

    pub fn get_value(&self, project_id: u64, table: &str, dict: &str, key: u64) -> Result<String> {
        let tx = self.db.transaction();
        let store_key = make_key_key(project_id, tbl_dict(table, dict).as_str(), key);
        match tx.get(store_key.as_slice())? {
            None => Err(MetadataError::NotFound(format!(
                "table {table}, project {project_id}, dict {dict}: value for key {key} not found"
            ))),
            Some(value) => Ok(String::from_utf8(value)?),
        }
    }

    pub fn get_key(&self, project_id: u64, table: &str, dict: &str, value: &str) -> Result<u64> {
        let tx = self.db.transaction();
        let store_key = make_value_key(project_id, tbl_dict(table, dict).as_str(), value);
        match tx.get(store_key.as_slice())? {
            None => Err(MetadataError::NotFound(format!(
                "table {table}, project {project_id}, dict {dict}: dictionary key by value {} not found",
                value
            ))),
            Some(key) => Ok(u64::from_le_bytes(key.try_into().unwrap())),
        }
    }
}

#[derive(Debug)]
pub struct SingleDictionaryProvider {
    pub project_id: u64,
    pub dict: String,
    pub table: String,
    pub dictionaries: Arc<Dictionaries>,
}

impl Hash for SingleDictionaryProvider {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.project_id.hash(state);
        self.dict.hash(state);
    }
}

impl SingleDictionaryProvider {
    pub fn new(
        project_id: u64,
        table: String,
        dict: String,
        dictionaries: Arc<Dictionaries>,
    ) -> Self {
        Self {
            project_id,
            dict,
            table,
            dictionaries,
        }
    }

    pub fn get_key_or_create(&self, value: &str) -> Result<u64> {
        self.dictionaries.get_key_or_create(
            self.project_id,
            self.table.as_str(),
            self.dict.as_str(),
            value,
        )
    }

    pub fn get_value(&self, key: u64) -> Result<String> {
        self.dictionaries.get_value(
            self.project_id,
            self.table.as_str(),
            self.dict.as_str(),
            key,
        )
    }

    pub fn get_key(&self, _project_id: u64, _dict: &str, value: &str) -> Result<u64> {
        self.dictionaries.get_key(
            self.project_id,
            self.table.as_str(),
            self.dict.as_str(),
            value,
        )
    }
}
