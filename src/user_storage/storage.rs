use crate::user_storage::memory::{Op, Memory, OpsBucket};
use arrow::datatypes::{Schema, SchemaRef, DataType};
use datafusion::scalar::ScalarValue;
use super::error::Result;
use core::mem;
use std::sync::mpsc::{channel, Sender, Receiver};
use std::cmp::Ordering;
use std::sync::{Arc, RwLock, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering as MemoryOrdering};
use arrow::record_batch::RecordBatch;
use crate::user_storage::error::{Error, ERR_TODO};
use arrow::record_batch::RecordBatchReader;
use parquet::file::reader::SerializedFileReader;
use parquet::file::writer::SerializedFileWriter;
use parquet::arrow::{ParquetFileArrowReader, ArrowReader, ArrowWriter};
use std::fs::File;
use std::path::Path;
use arrow::array::{build_compare, Array, ArrayRef, BooleanArray, DynComparator, Int8Array, MutableArrayData, StringArray, UInt8Array, BooleanBuilder};

#[derive(Clone)]
pub struct PropertyValue {
    property_id: usize,
    value: ScalarValue,
}

impl PropertyValue {
    fn new(property_id: usize, value: ScalarValue) -> Self {
        PropertyValue {
            property_id,
            value,
        }
    }
}

#[derive(Clone)]
pub enum Op {
    PutValues(Vec<PropertyValue>),
    IncrementValue {
        property_id: usize,
        delta: ScalarValue,
    },
    DecrementValue {
        property_id: usize,
        delta: ScalarValue,
    },
    DeleteUser(),
    // for buffer
    None,
}

#[derive(Clone)]
pub struct OrderedOp {
    order: usize,
    user_id: u64,
    pub op: Op,
}

impl OrderedOp {
    pub fn new(order: usize, user_id: u64, op: Op) -> Self {
        OrderedOp {
            order,
            user_id,
            op,
        }
    }

    pub fn new_empty() -> Self {
        OrderedOp { order: 0, user_id: 0, op: Op::None }
    }

    pub fn user_id_op(&self) -> (u64, Op) {
        (self.user_id, self.op.clone())
    }
}

impl PartialOrd for OrderedOp {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.user_id.partial_cmp(&other.user_id).unwrap().then(self.order.partial_cmp(&other.order).unwrap()))
    }
}

impl PartialEq for OrderedOp {
    fn eq(&self, other: &Self) -> bool {
        self.user_id == other.user_id && self.order == other.order
    }
}

impl Ord for OrderedOp {
    fn cmp(&self, other: &Self) -> Ordering {
        self.user_id.partial_cmp(&other.user_id).unwrap().then(self.order.partial_cmp(&other.order).unwrap())
    }
}

impl Eq for OrderedOp {}

#[derive(Clone)]
pub struct OpsBucket {
    pub idx: usize,
    pub schema: SchemaRef,
    pub users_count: usize,
    pub min_user_id: u64,
    pub max_user_id: u64,
    pub ops: Vec<(u64, Op)>,
}

#[derive(Clone)]
struct Bucket {
    id: usize,
    min_user_id: u64,
    max_user_id: u64,
}

impl Bucket {
    pub fn new(id: usize, min_user_id: u64, max_user_id: u64) -> Self {
        Bucket {
            id,
            min_user_id,
            max_user_id,
        }
    }
}

pub struct Storage {
    schema: SchemaRef,
    memory: Memory,
    flush_memory_buffer_sender: Sender<Memory>,
    flush_memory_buffer_receiver: Receiver<Memory>,
    merge_ops_bucket_sender: Sender<MergeOpsBucket>,
    write_buffer_size: usize,
    bucket_size: usize,
    next_bucket_id: AtomicUsize,
    index: RwLock<Vec<Bucket>>,
}

struct MergeOpsBucket {
    bucket_id: usize,
    ops_bucket: Arc<OpsBucket>,
}

impl MergeOpsBucket {
    fn new(bucket_id: usize, ops_bucket: Arc<OpsBucket>) -> Self {
        MergeOpsBucket {
            bucket_id,
            ops_bucket,
        }
    }
}

impl Storage {
    pub fn put_values(&mut self, user_id: u64, props: Vec<PropertyValue>) -> Result<()> {
        self.insert_op(user_id, Op::PutValues(props))
    }

    fn insert_op(&mut self, user_id: u64, op: Op) -> Result<()> {
        self.memory.insert_op(user_id, op)?;

        if self.memory.len() >= self.write_buffer_size {
            self.flush();
        }
        Ok(())
    }

    pub fn flush(&mut self) {
        let mut empty = self.memory.new_empty();
        let immutable = mem::replace(&mut self.memory, empty);
        self.flush_memory_buffer_sender.send(immutable).unwrap();
    }

    fn flush_memory_buffer(&mut self, mem: Memory) -> Result<()> {
        for bucket in self.split_rows_by_buckets(mem.ops()).unwrap().iter() {
            let mut bucket_id = self.next_bucket_id.fetch_add(1, MemoryOrdering::SeqCst);
            self.merge_ops_bucket_sender.send(MergeOpsBucket::new(bucket_id, bucket.clone()));
        }
        Ok(())
    }

    fn split_rows_by_buckets(&self, ordered_ops: &skiplist::OrderedSkipList<OrderedOp>) -> Result<Vec<Arc<OpsBucket>>> {
        let mut ret: Vec<Arc<OpsBucket>> = Vec::new();
        let mut ops = vec![(0u64, Op::None); self.bucket_size];
        let mut iter = ordered_ops.iter();
        let mut bucket_index: usize = 0;
        // bucket vars
        let mut users_count: usize = 0;
        let mut last_user_id: u64 = 0;
        let mut min_user_id: u64 = u64::MAX;
        let mut max_user_id: u64 = 0;
        ops.truncate(0);

        for op in ordered_ops.iter() {
            if last_user_id != op.user_id {
                users_count += 1;
            }
            if op.user_id < min_user_id {
                min_user_id = op.user_id;
            }
            if op.user_id > max_user_id {
                max_user_id = op.user_id;
            }

            if bucket_index != op.user_id as usize % self.bucket_size {
                if !ops.is_empty() {
                    let ops_bucket = OpsBucket {
                        idx: bucket_index,
                        schema: self.schema.clone(),
                        users_count,
                        min_user_id,
                        max_user_id,
                        ops: ops.clone(),
                    };
                    ret.push(Arc::new(ops_bucket));
                    ops.truncate(0);
                    users_count = 0;
                    min_user_id = u64::MAX;
                    max_user_id = 0;
                }
                bucket_index = op.user_id as usize % self.bucket_size;
            }
            ops.push(op.user_id_op());
        }

        if !ops.is_empty() {
            let ops_bucket = OpsBucket {
                idx: bucket_index,
                schema: self.schema.clone(),
                users_count,
                min_user_id,
                max_user_id,
                ops: ops.clone(),
            };
            ret.push(Arc::new(ops_bucket));
            ops.truncate(0);
        }

        Ok(ret)
    }

    fn merge_ops_bucket(&mut self, merge: MergeOpsBucket) -> Result<()> {
        let mut cur_data = RecordBatch::new_empty(self.schema.clone());
        match self.get_bucket_by_index(merge.ops_bucket.idx)? {
            None => {
                self.write_ops_bucket(merge.ops_bucket.clone())?
            }
            Some(bucket) => {
                self.merge_ops_bucket_with_existing(bucket, merge.ops_bucket.clone())?;
            }
        }
        Ok(())
    }

    fn merge_ops_bucket_with_existing(&mut self, bucket: Bucket, ops_bucket: Arc<OpsBucket>) -> Result<()> {
        let in_file = File::open("parquet.file").unwrap();
        let in_reader = SerializedFileReader::new(in_file).unwrap();
        let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(in_reader));
        let mut record_batch_reader = arrow_reader.get_record_reader(2048).unwrap();

        let out_file = File::create("parquet.file").unwrap();
        let mut rb_writer = ArrowWriter::try_new(out_file, ops_bucket.schema.clone(), None).unwrap();
        for b in record_batch_reader {
            let batch = b.unwrap();
        }
        Ok(())
    }

    fn write_ops_bucket(&mut self, bucket: Arc<OpsBucket>) -> Result<()> {
        let out_file = File::create(Path::new(&bucket.idx.to_string())).unwrap();
        let mut rb_writer = ArrowWriter::try_new(out_file, bucket.schema.clone(), None).unwrap();
        /*let mut row: Vec<ScalarValue> = ops_bucket.schema.fields().iter().map(|field| {
            match field.data_type() {
                DataType::Int8 => {
                    ScalarValue::Int8(None)
                }
                _ => {
                    todo!();
                }
            }
        }).collect();

        let mut out = ops_bucket.schema.fields().iter().map(|field| {
            match field.data_type() {
                DataType::Int8 => Int8Array::builder(ops_bucket.ops.len()),
                _ => {
                    todo!();
                }
            }
        }).collect();
        let last_user_id: u64 = 0;
        for (idx, (user_id, op)) in ops_bucket.ops.iter().enumerate() {
            if idx > 0 && last_user_id != *user_id {
                for (idx, v) in row.iter_mut().enumerate() {
                    match v.get_datatype() {
                        DataType::Int8 => {
                            ScalarValue::Int8(None);
                        }
                        _ => {
                            todo!();
                        }
                    }
                }
                let d = row.iter().map(|v| {}).collect();
            }
            match op {
                Op::PutValues(props) => {}
                Op::IncrementValue { .. } => {}
                Op::DecrementValue { .. } => {}
                Op::DeleteUser() => {}
                Op::None => {}
            }
            rb_writer.write().unwrap();
        }*/

        let mut pre_cols = vec![0usize; self.schema.fields().len()];
        for (_, op) in bucket.ops.iter() {
            match op {
                Op::PutValues(props) => {
                    for v in props.iter() {
                        pre_cols[v.property_id] += 1;
                    }
                }
                Op::IncrementValue { property_id, delta } => {
                    pre_cols[*property_id] += 1;
                }
                Op::DecrementValue { property_id, delta } => {
                    pre_cols[*property_id] += 1;
                }
                _ => {}
            }
        }
        cols = pre_cols.iter().enumerate().filter_map(| (col_id, n) | {

        });

        let users_count: usize = 0;
        let cols = self.schema.fields().iter().enumerate().map(|(col_id, field)| {
            match field.data_type() {
                DataType::Boolean => {
                    // use buffer because standard is slower
                    let mut builder = BooleanBuilder::new(bucket.users_count);
                    let mut v = ScalarValue::Boolean(None);
                    let mut last_user_id: u64 = 0;
                    for (user_id, op) in bucket.ops.iter() {
                        if *user_id != last_user_id {}
                        match op {
                            Op::PutValues(v) => {}
                            Op::IncrementValue { .. } => {}
                            Op::DecrementValue { .. } => {}
                            Op::DeleteUser() => {}
                            Op::None => {}
                        }
                        // columns can't be deleted within current memory instance, so we can check column presence like this
                        if col_id < row.len() - 1 {
                            let v = if let ScalarValue::Boolean(v) = row[col_id] { v } else { panic!("error casting to Boolean") };
                            builder.append_option(v).unwrap();
                        } else {
                            builder.append_null();
                        }
                    }
                    Arc::new(builder.finish()) as ArrayRef
                }
                DataType::Int8 => {
                    let mut builder = Int8Builder::new(self.rows.len());
                    for row in self.rows.iter() {
                        if col_id < row.len() - 1 {
                            let v = if let ScalarValue::Int8(v) = row[col_id] { v } else { panic!("error casting to Int8") };
                            builder.append_option(v).unwrap();
                        } else {
                            builder.append_null();
                        }
                    }
                    Arc::new(builder.finish()) as ArrayRef
                }
                _ => unimplemented!()
            }
        }).collect();

        RecordBatch::try_new(self.schema.clone(), cols).unwrap()

        Ok(())
    }

    fn get_bucket_by_index(&self, idx: usize) -> Result<Option<Bucket>> {
        let buckets = self.index.read().unwrap();
        if buckets.len() - 1 < idx {
            if idx - (buckets.len() - 1) > 1 {
                Err(Error::Internal(ERR_TODO))
            } else {
                Ok(None)
            }
        } else {
            Ok(Some(buckets[idx].clone()))
        }
    }
}