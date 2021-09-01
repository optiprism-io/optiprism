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

struct Oplog {
    chunk_size: usize,
    // each column contains oplog
    columns: Vec<BTreeMap<u64, ScalarValue>>,
}

impl Oplog {
    pub fn set(&mut self, col_id: usize, index: u64, value: ScalarValue) {
        let batch_id = index / self.chunk_size;
        let offset = index % self.chunk_size;

        self.columns[col_id as usize][batch_id as usize].insert(offset, value);
    }
}

impl Oplog {
    pub fn new(cols: usize, chunks: usize, chunk_size: usize) -> Self {
        Oplog {
            chunk_size,
            columns: vec![BTreeMap::new(); cols],
        }
    }
}

enum ChunkData {
    Empty,
    Arrow(ArrayRef),
}

struct Chunk {
    data: ChunkData,
    min_key: u64,
    max_key: u64,
}

struct Column {
    data_type: DataType,
    chunks: Vec<RwLock<Chunk>>,
}

struct Store {
    columns: Vec<Column>,
    db: Arc<DB>,
    oplog: Mutex<Oplog>,
    chunk_size: usize,
    len: usize,
}

struct ColumnDescriptor {
    data_type: DataType,
    index: usize,
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

impl Into<ScalarValue> for DataValue {
    fn into(self) -> ScalarValue {
        match self {
            DataValue::Boolean(v) => ScalarValue::Boolean(v),
            DataValue::Int8(v) => ScalarValue::Int8(v),
        }
    }
}

impl Store {
    // persist values in RocksDB and in in-memory oplog
    // todo think about transactions
    pub fn set(&mut self, index: usize, values: Vec<(&ColumnDescriptor, &ScalarValue)>) -> Result<()> {
        // lock oplog for write
        let oplog = self.oplog.lock().unwrap();
        let mut batch = WriteBatch::default();
        for (cd, value) in values.iter() {
            let data_value = DataValue::from(*value);
            // write to rocksdb in corresponding column
            batch.put_cf(
                self.db.cf_handle(cd.index.to_string().as_ref()).unwrap(),
                index.to_le_bytes(),
                bincode::serialize(&data_value).unwrap(),
            );

            // insert/update value in oplog
            oplog.columns[cd.index].insert(*index, (*value).clone());
        }
        // Atomically commits the batch
        self.db.write(batch).unwrap();
        Ok(())
    }

    pub fn get(&mut self, index: usize, cds: Vec<&ColumnDescriptor>) -> Result<Vec<Option<ScalarValue>>> {
        let rocks_cds = cds
            .iter()
            .map(|cd| self.db.cf_handle(cd.index.to_string().as_ref()).unwrap())
            .collect();
        let keys = vec![index; rocks_cds.len()];

        self.db.multi_get_cf(&[rocks_cds, keys]).iter().map(|value| {
            match value.or_else(|e| e).unwrap() {
                None => Ok(None),
                Some(v) => Ok(Some(DataValue::deserialize(&v).unwrap().into()))
            }
        }).collect()?
    }

    fn merge(&mut self) -> Result<()> {
        let mut guard = self.oplog.lock().unwrap();
        let oplog = std::mem::replace(&mut *guard, Oplog::new(self.columns.len()));
        for (col_id, chunks) in oplog.columns.iter().enumerate() {
            for (chunk_id, chunk) in chunks.iter().enumerate() {
                let mut data_chunk = self.columns[col_id].chunks[chunk_id].write().unwrap();


                match data_chunk {}
            }
            let mut chunk_id: usize = 0;
            let mut chunk_guard: Option<RwLockWriteGuard<Chunk>> = None;
            for (index, v) in col_data.iter() {
                match chunk_guard {
                    None => {
                        chunk_guard = Some()
                    }
                    Some(_) => {}
                }
                chunk_id = index / self.chunk_size;
            }
        }

        Ok(())
    }
}
