#![feature(slice_take)]
#![feature(let_chains)]
#![feature(allocator_api)]
#![feature(pattern)]

extern crate core;

pub mod arrow_conversion;
mod compaction;
pub mod db;
pub mod error;
pub(crate) mod memtable;
pub mod options;
pub mod parquet;
pub mod table;
pub mod test_util;

use std::collections::HashMap;
use std::fmt::Debug;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::RwLock;

use error::Result;
use get_size::GetSize;
use serde::Deserialize;
use serde::Serialize;

use crate::error::StoreError;

pub mod metadata {
    include!(concat!(env!("OUT_DIR"), "/metadata.rs"));
}

#[derive(Serialize, Deserialize, Eq, PartialEq, PartialOrd, Ord, Debug, Clone, Hash, GetSize)]
pub enum KeyValue {
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    String(String),
    Timestamp(i64),
}

impl TryFrom<&parquet::ParquetValue> for KeyValue {
    type Error = StoreError;

    fn try_from(value: &parquet::ParquetValue) -> std::result::Result<Self, Self::Error> {
        Ok(match value {
            parquet::ParquetValue::Int32(v) => KeyValue::Int32(*v),
            parquet::ParquetValue::Int64(v) => KeyValue::Int64(*v),
            _ => unimplemented!(),
        })
    }
}

#[derive(Clone, Debug)]
pub struct NamedValue {
    name: String,
    value: Value,
}

impl NamedValue {
    pub fn new(name: String, value: Value) -> Self {
        Self { name, value }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Hash, GetSize, PartialEq, Eq)]
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

impl From<&KeyValue> for Value {
    fn from(value: &KeyValue) -> Self {
        match value {
            KeyValue::Int8(v) => Value::Int8(Some(*v)),
            KeyValue::Int16(v) => Value::Int16(Some(*v)),
            KeyValue::Int64(v) => Value::Int64(Some(*v)),
            KeyValue::String(v) => Value::String(Some(v.to_owned())),
            KeyValue::Int32(v) => Value::Int32(Some(*v)),
            KeyValue::Timestamp(v) => Value::Timestamp(Some(*v)),
        }
    }
}

impl From<&Value> for KeyValue {
    fn from(value: &Value) -> Self {
        match value {
            Value::Int8(Some(v)) => KeyValue::Int8(*v),
            Value::Int16(Some(v)) => KeyValue::Int16(*v),
            Value::Int64(Some(v)) => KeyValue::Int64(*v),
            Value::String(Some(v)) => KeyValue::String(v.to_owned()),
            Value::Int32(Some(v)) => KeyValue::Int32(*v),
            Value::Timestamp(Some(v)) => KeyValue::Timestamp(*v),
            _ => unreachable!("{:?}", value),
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Stats {
    pub(crate) resident_bytes: u64,
    pub(crate) on_disk_bytes: u64,
    pub(crate) logged_bytes: u64,
    pub(crate) written_bytes: u64,
    pub(crate) read_bytes: u64,
    pub(crate) space_amp: u64,
    pub(crate) write_amp: u64,
}

#[derive(Debug)]
pub struct Fs {
    count: Arc<RwLock<HashMap<String, usize>>>,
}

impl Default for Fs {
    fn default() -> Self {
        Self::new()
    }
}

impl Fs {
    pub fn new() -> Self {
        Self {
            count: Default::default(),
        }
    }

    pub fn open<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let mut count = self.count.write().unwrap();
        let c = count
            .entry(path.as_ref().display().to_string())
            .or_insert(0);
        *c += 1;
        Ok(())
    }

    pub fn close<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let mut count = self.count.write().unwrap();
        let c = count
            .entry(path.as_ref().display().to_string())
            .or_insert(0);
        *c -= 1;
        Ok(())
    }

    pub fn is_opened<P: AsRef<Path>>(&self, path: P) -> bool {
        let count = self.count.read().unwrap();
        match count.get(&path.as_ref().display().to_string()) {
            None => false,
            Some(v) => *v != 0,
        }
    }
    pub fn try_remove_file<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        if !self.is_opened(&path) {
            return Ok(fs::remove_file(path)?);
        }
        Ok(())
    }

    pub fn remove_file<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        Ok(fs::remove_file(path)?)
    }

    pub fn _try_rename<P: AsRef<Path>>(&self, from: P, to: P) -> Result<()> {
        if !self.is_opened(&from) && !self.is_opened(&to) {
            return Ok(fs::rename(from, to)?);
        }

        Ok(())
    }

    pub fn rename<P: AsRef<Path>>(&self, from: P, to: P) -> Result<()> {
        Ok(fs::rename(from, to)?)
    }
}

#[derive(Debug, Clone)]
pub(crate) enum FsOp {
    Rename(PathBuf, PathBuf),
    Delete(PathBuf),
}
