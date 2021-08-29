use arrow::datatypes::{DataType, SchemaRef};
use std::collections::HashMap;
use std::any::Any;
use std::sync::{Arc, RwLock};
use datafusion::scalar::ScalarValue;
use super::{
    error::{Result},
};
use rocksdb::DB;
use serde::{Deserialize, Serialize};
use datafusion::datasource::TableProvider;
use datafusion::logical_plan::Expr;
use datafusion::datasource::datasource::Statistics;
use datafusion::physical_plan::{ExecutionPlan, Partitioning, SendableRecordBatchStream};
use std::rc::Rc;
use crate::user_storage::error::{InternalError, Error};
use arrow::record_batch::RecordBatch;

enum ChunkData {
    Boolean(Vec<bool>),
    Int8(Vec<i8>),
}

enum ColumnChunk {
    // chunk without any value
    Empty,
    // chunk not in memory
    Unloaded,
    Uncompressed(UncompressedChunk),
}

struct UncompressedChunk {
    data: ChunkData,
    offset: usize,
    len: usize,
    is_nullable: bool,
    nulls: Vec<bool>,
}

impl UncompressedChunk {
    pub fn set(&mut self, index: usize, value: &ScalarValue) {
        let index = index - self.offset;
        match &mut self.data {
            ChunkData::Boolean(vals) => {
                if let ScalarValue::Boolean(v) = value {
                    match v {
                        Some(x) => {
                            vals[index] = *x;
                            if self.is_nullable {
                                self.nulls[index] = false;
                            }
                        }
                        None => {
                            if self.is_nullable {
                                self.nulls[index] = true;
                            }
                        }
                    }
                }
            }
            ChunkData::Int8(vals) => {
                if let ScalarValue::Int8(v) = value {
                    match v {
                        Some(x) => {
                            vals[index] = *x;
                            if self.is_nullable {
                                self.nulls[index] = false;
                            }
                        }
                        None => {
                            if self.is_nullable {
                                self.nulls[index] = true;
                            }
                        }
                    }
                }
            }
        }
    }

    pub fn get(&self, index: usize) -> ScalarValue {
        let index = index - self.offset;
        assert_eq!(index <= self.len - 1);
        match &self.data {
            ChunkData::Boolean(v) => {
                if self.is_nullable && self.nulls[index] {
                    ScalarValue::Boolean(None)
                } else {
                    ScalarValue::Boolean(Some(v[index]))
                }
            }
            ChunkData::Int8(v) => {
                if self.is_nullable && self.nulls[index] {
                    ScalarValue::Int8(None)
                } else {
                    ScalarValue::Int8(Some(v[index]))
                }
            }
        }
    }
}

impl ColumnChunk {
    pub fn new_empty_uncompressed(data_type: &DataType, is_nullable: bool, size: usize, len: usize, offset: usize) -> ColumnChunk {
        let nulls = match is_nullable {
            true => vec![true; size],
            false => vec![false; 0], // more convenient than Option
        };

        match data_type {
            DataType::Boolean => ColumnChunk::Uncompressed(UncompressedChunk {
                data: ChunkData::Boolean(vec![false; size]),
                offset,
                nulls,
                is_nullable,
                len,
            }),
            DataType::Int8 => ColumnChunk::Uncompressed(UncompressedChunk {
                data: ChunkData::Int8(vec![0; size]),
                offset,
                nulls,
                is_nullable,
                len,
            }),
            _ => panic!("unimplemented")
        }
    }
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

struct Column {
    data_type: DataType,
    is_nullable: bool,
    chunk_lock: Vec<RwLock<()>>,
    chunks: Vec<Rc<ColumnChunk>>,
    chunk_size: usize,
    len: usize,
}

impl Column {
    pub fn make_missing_chunks(&mut self, index: usize) {
        let missing_chunks = self.chunks.len() - index / self.chunk_size;
        if missing_chunks > 0 {
            for _ in 0..missing_chunks {
                self.chunks.push(Rc::new(ColumnChunk::Empty))
            }
        }
    }

    pub fn set(&mut self, index: usize, value: &ScalarValue) {
        assert_eq!(self.chunks.len() - 1 >= index / self.chunk_size);
        if let Some(chunk) = self.get_chunk_by_index(index).as_mut() {
            if let ColumnChunk::Uncompressed(mut chunk) = chunk {
                chunk.set(index, value);
            }
        }
    }

    pub fn get(&mut self, index: usize) -> Option<ScalarValue> {
        if let Some(chunk) = self.get_chunk_by_index(index).as_mut() {
            if let ColumnChunk::Uncompressed(chunk) = chunk {
                return Some(chunk.get(index));
            }
        }

        None
    }

    fn get_chunk_by_index(&self, index: usize) -> Rc<ColumnChunk> {
        let chunk_id = index / self.chunk_size;
        return self.chunks[chunk_id].clone();
    }

    pub fn chunk_iter_mut(&mut self, column_len: usize) -> ColumnIter {
        ColumnIter { col: self }
    }
}

struct ColumnIter<'a> {
    col: &'a mut Column,
}

impl<'a> Iterator for ColumnIter<'a> {
    type Item = Option<ChunkData>;

    fn next(&mut self) -> Option<Self::Item> {
    }
}

struct Store {
    db: Arc<DB>,
    chunk_size: usize,
    columns: HashMap<DataType, Vec<Column>>,
    len: usize,
}

struct ColumnDescriptor {
    data_type: DataType,
    offset: usize,
}

impl ColumnDescriptor {
    pub fn make_key(&self) -> String {
        let dt = match self.data_type {
            DataType::Boolean => "boolean",
            DataType::Int8 => "int8",
            _ => panic!("unimplemented"),
        };

        format!("properties_{}_{}", dt, self.offset)
    }
}

impl Store {
    pub fn new(db: Arc<DB>, chunk_size: usize, cds: Vec<ColumnDescriptor>) -> Self {
        let columns: HashMap<DataType, Vec<Column>> = HashMap::new();

        for cd in cds.iter() {}
        Store {
            db: db.clone(),
            chunk_size,
            columns,
            len: 0,
        }
    }

    pub fn push(&mut self, values: Vec<(&ColumnDescriptor, &ScalarValue)>) -> Result<usize> {
        self.len += 1;
        self.db.put(
            "len",
            self.len.to_le_bytes(),
        ).unwrap();

        for (cd, value) in values.iter() {
            self.set(cd, self.len - 1, value).unwrap()
        }
        Ok(self.len - 1)
    }

    pub fn set(&mut self, cd: &ColumnDescriptor, index: usize, value: &ScalarValue) -> Result<()> {
        let type_cols = self.columns.get(&cd.data_type).unwrap();
        let mut col = &type_cols[cd.offset];
        assert_eq!(index <= col.len - 1);
        let data_value = DataValue::from(&value);
        self.db.put_cf(
            self.db.cf_handle(cd.make_key().as_ref()).unwrap(),
            index.to_le_bytes(),
            bincode::serialize(&data_value).unwrap(),
        );

        col.set(index, value);
        Ok(())
    }

    pub fn multi_set(&mut self, index: usize, values: Vec<(&ColumnDescriptor, &ScalarValue)>) -> Result<()> {
        for (cd, value) in values.iter() {
            self.set(cd, index, value).unwrap();
        }

        Ok(())
    }

    pub fn get(&mut self, cd: &ColumnDescriptor, index: usize) -> Result<Option<ScalarValue>> {
        let type_cols = self.columns.get(&cd.data_type).unwrap();
        let mut col = &type_cols[cd.offset];
        if let Ok(v) = col.get(index) {
            return Ok(v);
        }

        let value = self.db.get_cf(
            self.db.cf_handle(cd.make_key().as_ref()).unwrap(),
            index.to_le_bytes(),
        ).unwrap();

        match value {
            None => Ok(None),
            Some(v) => {
                Ok(Some(DataValue::deserialize(&v).unwrap().into()))
            }
        }
    }

    pub fn multi_get(&mut self, index: usize, values: Vec<(&ColumnDescriptor, &ScalarValue)>) -> Result<Vec<Option<ScalarValue>>> {
        let mut res: Vec<Option<ScalarValue>> = vec![None, values.len()];
        for (cd, value) in values.iter() {
            res.push(self.get(*cd, *index).unwrap());
        }

        Ok(res)
    }

    pub fn scan_all(&mut self, cds: Vec<&ColumnDescriptor>) {
        let mut cursor: usize = 0;

        while cursor <= self.len - 1 {
            for cd in cds.iter() {
                let type_cols = self.columns.get(&cd.data_type).unwrap();
                let mut col = &type_cols[cd.offset];
                for chunk in col.chunk_iterator(self.len) {}
            }


            cursor += 1;
        }
    }
}

struct ColumnReader {
    column: Column,
}

impl ExecutionPlan for ColumnReader {
    fn as_any(&self) -> &dyn Any {
        todo!()
    }

    fn schema(&self) -> SchemaRef {
        todo!()
    }

    fn output_partitioning(&self) -> Partitioning {
        todo!()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        todo!()
    }

    fn with_new_children(&self, children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> {
        todo!()
    }

    async fn execute(&self, _: usize) -> Result<SendableRecordBatchStream> {
    }
}

impl TableProvider for Store {
    fn as_any(&self) -> &dyn Any {
        todo!()
    }

    fn schema(&self) -> SchemaRef {
        todo!()
    }

    fn scan(&self, projection: &Option<Vec<usize>>, batch_size: usize, filters: &[Expr], limit: Option<usize>) -> Result<Arc<dyn ExecutionPlan>> {
        todo!()
    }

    fn statistics(&self) -> Statistics {
        todo!()
    }
}
