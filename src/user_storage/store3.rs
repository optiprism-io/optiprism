use std::sync::{Arc, RwLock, Mutex, RwLockWriteGuard};
use std::collections::BTreeMap;
use serde::{Deserialize, Serialize};
use datafusion::scalar::ScalarValue;
use super::{
    error::{Result},
};
use arrow::datatypes::{DataType, SchemaRef};
use rocksdb::{DB, WriteBatch, Error};
use arrow::array::ArrayRef;
use arrow::array::{Array, UInt32Array};
use parquet::file::reader::ChunkReader;

const CHUNK_SIZE: usize = 1_000;

struct Oplog {
    db: DB,
    // each column contains oplog
    columns: Vec<BTreeMap<u64, ScalarValue>>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
enum DataValue {
    Boolean(Option<bool>),
    Int8(Option<i8>),
}

impl DataValue {
    fn deserialize(bytes: &[u8]) -> Result<Self> {
        Ok(bincode::deserialize(bytes).unwrap())
    }
}

impl From<&ScalarValue> for DataValue {
    fn from(v: &ScalarValue) -> Self {
        match v {
            ScalarValue::Boolean(x) => DataValue::Boolean(x.clone()),
            ScalarValue::Int8(x) => DataValue::Int8(x.clone()),
            _ => panic!("unimplemented"),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct DataOp {
    key: u64,
    value: DataValue,
}

impl DataOp {
    pub fn new(key: u64, value: DataValue) -> Self {
        DataOp {
            key,
            value,
        }
    }
}

impl Oplog {
    pub fn set(&mut self, col_id: usize, key: u64, value: ScalarValue) {
        let data = DataOp::new(key, DataValue::from(&value));
        self.db.put(col_id.to_le_bytes(), bincode::serialize(&data).unwrap()).unwrap();
        self.columns[col_id as usize].insert(key, value);
    }
}

impl Oplog {
    pub fn new(db: DB, cols: usize) -> Self {
        Oplog {
            db,
            columns: vec![BTreeMap::new(); cols],
        }
    }
}

struct Store {
    oplog: Oplog,
    chunk_size: usize,
}

impl Store {
    pub fn merge(&mut self) {
        for column in self.oplog.columns.iter() {
            let mut chunk_id: usize = 0;
            let mut chunk: Vec<(u64, ScalarValue)> = vec![(0, ScalarValue::Boolean(None)); CHUNK_SIZE];
            let mut chunk_size: usize = 0;
            for (key, value) in column {
                if self.chunk_id_for_key(*key) > chunk_id && chunk_size > 0 {} else {
                    chunk_size += 1;
                    chunk.push((*key, value.clone()))
                }
            }
        }
    }

    fn get_chunk_by_id(&self, chunk_id: usize) -> Option<ArrayRef> {
        let b:ArrayRef;
        parquet::column::writer::get_column_writer()
    }
    fn chunk_id_for_key(&self, key: u64) -> usize {
        return (key / self.chunk_size) as usize;
    }
}