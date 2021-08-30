use std::sync::{Arc, RwLock};
use std::collections::BTreeMap;
use serde::{Deserialize, Serialize};
use datafusion::scalar::ScalarValue;
use super::{
    error::{Result},
};
use arrow::datatypes::{DataType, SchemaRef};
use rocksdb::{DB, WriteBatch, Error};
use std::cell::RefCell;

struct Oplog {
    // we use btreemap to be able to sort values by key
    columns: Vec<RefCell<BTreeMap<u64, ScalarValue>>>,
}

struct Store {
    db: Arc<DB>,
    oplog: RwLock<Oplog>,
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
        let oplog = self.oplog.write().unwrap();
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
            match value {
                Ok(v) => {
                    match v {
                        None => Ok(None),
                        Some(v) => Ok(Some(DataValue::deserialize(&v).unwrap().into()))
                    }
                }
                Err(err) => Err(err)
            }
        }).collect()?
    }
}
