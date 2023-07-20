use core::mem;use std::cmp::Ordering;
use std::fs::File;
use std::ops::AddAssign;
use std::ops::SubAssign;
use std::path::Path;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering as MemoryOrdering;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::sync::RwLock;

use arrow::array::Array;
use arrow::array::ArrayRef;
use arrow::array::BooleanBuilder;
use arrow::array::Int8Array;
use arrow::array::Int8Builder;
use arrow::array::PrimitiveBuilder;
use arrow::array::UInt64Array;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use arrow::datatypes::UInt64Type;
use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion_common::ScalarValue;
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use parquet::arrow::ArrowWriter;

use super::error::Result;
use crate::sorted_hash_map::error::Error;
use crate::sorted_hash_map::error::ERR_TODO;
use crate::sorted_hash_map::memory::Memory;

#[derive(Clone)]
pub struct Value {
    col_id: usize,
    value: ScalarValue,
}

impl Value {
    fn new(col_id: usize, value: ScalarValue) -> Self {
        Value { col_id, value }
    }
}

#[derive(Clone)]
pub enum Op {
    PutValues(Vec<Option<ScalarValue>>),
    IncrementValue { col_id: usize, delta: ScalarValue },
    DecrementValue { col_id: usize, delta: ScalarValue },
    DeleteKey,
    None,
}

#[derive(Clone)]
pub struct OrderedOp {
    order: usize,
    key: u64,
    pub op: Op,
}

impl OrderedOp {
    pub fn new(order: usize, key_id: u64, op: Op) -> Self {
        OrderedOp {
            order,
            key: key_id,
            op,
        }
    }

    pub fn keyed_op(&self) -> (u64, Op) {
        (self.key, self.op.clone())
    }
}

impl PartialOrd for OrderedOp {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(
            self.key
                .partial_cmp(&other.key)
                .unwrap()
                .then(self.order.partial_cmp(&other.order).unwrap()),
        )
    }
}

impl PartialEq for OrderedOp {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.order == other.order
    }
}

impl Ord for OrderedOp {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key
            .partial_cmp(&other.key)
            .unwrap()
            .then(self.order.partial_cmp(&other.order).unwrap())
    }
}

impl Eq for OrderedOp {}

#[derive(Clone)]
pub struct OpsBucket {
    pub idx: usize,
    // schema for current bucket (noref)
    pub schema: Schema,
    pub keys_count: usize,
    pub min_key: u64,
    pub max_key: u64,
    pub ops: Vec<(u64, Op)>,
}

impl OpsBucket {
    pub fn to_record_batch(&self) -> Result<RecordBatch> {
        let mut key_col = UInt64Array::builder(self.keys_count);

        let mut cols: Vec<ArrayRef> = self
            .schema
            .fields()
            .iter()
            .enumerate()
            .map(|(col_id, field)| {
                match field.data_type() {
                    DataType::Boolean => {
                        // use buffer because standard is slower
                        let mut builder = BooleanBuilder::with_capacity(self.keys_count);
                        let mut last_key: u64 = 0;
                        let mut value = MergeValue::<bool>::new_null();
                        for (key, op) in self.ops.iter() {
                            if *key != last_key {
                                if value.is_set {
                                    if value.is_null {
                                        builder.append_null();
                                    } else {
                                        builder.append_value(value.value.clone());
                                    }
                                    value.reset();
                                    last_key = *key;
                                    if col_id == 1 {
                                        key_col.append_value(*key);
                                    }
                                }
                            }

                            match op {
                                Op::PutValues(vals) => {
                                    if col_id <= vals.len() - 1 && vals[col_id].is_some() {
                                        if let ScalarValue::Boolean(v) =
                                            vals[col_id].clone().unwrap()
                                        {
                                            value.set_option(v);
                                        }
                                    }
                                }
                                Op::IncrementValue { .. } => unreachable!(),
                                Op::DecrementValue { .. } => unreachable!(),
                                Op::DeleteKey => value.unset(),
                                Op::None => {}
                            }
                        }

                        if value.is_set {
                            if value.is_null {
                                builder.append_null();
                            } else {
                                builder.append_value(value.value.clone());
                            }
                            if col_id == 1 {
                                key_col.append_value(last_key);
                            }
                        }
                        Arc::new(builder.finish()) as ArrayRef
                    }
                    DataType::Int8 => {
                        // use buffer because standard is slower
                        let mut builder = Int8Builder::with_capacity(self.keys_count);
                        let mut last_key: u64 = 0;
                        let mut value = MergeValue::<i8>::new_null();
                        let mut first = true;
                        for (key, op) in self.ops.iter() {
                            if *key != last_key {
                                if !first {
                                    if value.is_set {
                                        if value.is_null {
                                            builder.append_null();
                                        } else {
                                            builder.append_value(value.value.clone());
                                        }
                                        if col_id == 1 {
                                            key_col.append_value(last_key);
                                        }
                                    }
                                }
                                value.reset();
                                last_key = *key;
                                first = false;
                            }

                            match op {
                                Op::PutValues(vals) => {
                                    if col_id <= vals.len() - 1 && vals[col_id].is_some() {
                                        if let ScalarValue::Int8(v) = vals[col_id].clone().unwrap()
                                        {
                                            value.set_option(v);
                                        }
                                    }
                                }
                                Op::IncrementValue {
                                    col_id: inc_col_id,
                                    delta,
                                } => {
                                    if col_id == *inc_col_id {
                                        if let ScalarValue::Int8(v) = delta {
                                            value.inc(v.unwrap());
                                        }
                                    }
                                }
                                Op::DecrementValue {
                                    col_id: inc_col_id,
                                    delta,
                                } => {
                                    if col_id == *inc_col_id {
                                        if let ScalarValue::Int8(v) = delta {
                                            value.dec(v.unwrap());
                                        }
                                    }
                                }
                                Op::DeleteKey => {
                                    value.unset();
                                }
                                Op::None => {}
                            }
                        }

                        if value.is_set {
                            if value.is_null {
                                builder.append_null();
                            } else {
                                builder.append_value(value.value.clone());
                            }
                            if col_id == 1 {
                                key_col.append_value(last_key);
                            }
                        }

                        Arc::new(builder.finish()) as ArrayRef
                    }
                    _ => unimplemented!(),
                }
            })
            .collect();

        let schema = Schema::try_merge(vec![
            Schema::new(vec![Field::new("key", DataType::UInt64, false)]),
            self.schema.clone(),
        ])
        .unwrap();

        let mut final_cols = vec![Arc::new(key_col.finish()) as ArrayRef];
        final_cols.append(&mut cols);
        Ok(RecordBatch::try_new(Arc::new(schema), final_cols).unwrap())
    }

    fn merge_i8_columns(
        &self,
        field: &Field,
        left_col_id: usize,
        right_col: &ChunkedArray<Int8Array>,
        right_key_col: &ChunkedArray<UInt64Array>,
        result_key_col: &mut PrimitiveBuilder<UInt64Type>,
        bucket_size: usize,
        is_first: bool,
    ) -> Int8Array {
        // use buffer because standard is slower
        let mut res_builder = Int8Builder::with_capacity(bucket_size);
        let mut left_idx: usize = 0;
        let mut right_idx: usize = 0;

        let mut value = MergeValue::<i8>::new_null();
        while left_idx <= self.ops.len() - 1 && right_idx <= right_key_col.len() - 1 {
            value.reset();
            let left_key = self.ops[left_idx].0;
            let right_key = right_key_col.value(right_idx);
            let mut final_key: u64 = 0;
            match left_key.cmp(&right_key) {
                Ordering::Less => {
                    merge_i8_value(&self.ops, left_key, &mut left_idx, &mut value, left_col_id);
                    final_key = left_key;
                    left_idx += 1;
                }
                Ordering::Equal => {
                    value.is_null = right_col.is_null(right_idx);
                    value.value = right_col.value(right_idx);
                    merge_i8_value(&self.ops, left_key, &mut left_idx, &mut value, left_col_id);

                    final_key = left_key;
                    left_idx += 1;
                    right_idx += 1;
                }
                Ordering::Greater => {
                    value.is_null = right_col.is_null(right_idx);
                    value.value = right_col.value(right_idx);
                    final_key = right_key;
                    right_idx += 1;
                }
            }

            if value.is_set {
                if value.is_null {
                    res_builder.append_null();
                } else {
                    res_builder.append_value(value.value.clone());
                }
                if is_first {
                    result_key_col.append_value(final_key);
                }
            }
        }

        while left_idx <= self.ops.len() - 1 {
            value.reset();
            let key = self.ops[left_idx].0;
            merge_i8_value(&self.ops, key, &mut left_idx, &mut value, left_col_id);
            left_idx += 1;
            if value.is_set {
                if value.is_null {
                    res_builder.append_null();
                } else {
                    res_builder.append_value(value.value.clone());
                }
                if is_first {
                    result_key_col.append_value(key);
                }
            }
        }

        while right_idx <= right_col.len() - 1 {
            value.is_null = right_col.is_null(right_idx);
            value.value = right_col.value(right_idx);
            right_idx += 1;
            if value.is_set {
                if value.is_null {
                    res_builder.append_null();
                } else {
                    res_builder.append_value(value.value.clone());
                }
                if is_first {
                    result_key_col.append_value(right_key_col.value(right_idx));
                }
            }
        }

        res_builder.finish()
    }

    fn apply_left_i8_column(
        &self,
        field: &Field,
        left_col_id: usize,
        right_key_col: &ChunkedArray<UInt64Array>,
        result_key_col: &mut PrimitiveBuilder<UInt64Type>,
        bucket_size: usize,
        is_first: bool,
    ) -> Int8Array {
        // use buffer because standard is slower
        let mut res_builder = Int8Builder::with_capacity(bucket_size);
        let mut left_idx: usize = 0;
        let mut right_idx: usize = 0;

        let mut value = MergeValue::<i8>::new_null();
        while left_idx <= self.ops.len() - 1 && right_idx <= right_key_col.len() - 1 {
            value.reset();
            let left_key = self.ops[left_idx].0;
            let right_key = right_key_col.value(right_idx);
            let mut final_key: u64 = 0;
            match left_key.cmp(&right_key) {
                Ordering::Less => {
                    merge_i8_value(&self.ops, left_key, &mut left_idx, &mut value, left_col_id);
                    final_key = left_key;
                    left_idx += 1;
                }
                Ordering::Equal => {
                    merge_i8_value(&self.ops, left_key, &mut left_idx, &mut value, left_col_id);
                    final_key = left_key;
                    left_idx += 1;
                    right_idx += 1;
                }
                Ordering::Greater => {
                    final_key = right_key;
                    right_idx += 1;
                }
            }

            if value.is_set {
                if value.is_null {
                    res_builder.append_null();
                } else {
                    res_builder.append_value(value.value.clone());
                }
                if is_first {
                    result_key_col.append_value(final_key);
                }
            }
        }

        while left_idx <= self.ops.len() - 1 {
            let key = self.ops[left_idx].0;
            merge_i8_value(&self.ops, key, &mut left_idx, &mut value, left_col_id);
            left_idx += 1;
            if value.is_set {
                if value.is_null {
                    res_builder.append_null();
                } else {
                    res_builder.append_value(value.value.clone());
                }
                if is_first {
                    result_key_col.append_value(key);
                }
            }
        }

        res_builder.finish()
    }

    fn apply_right_i8_column(
        &self,
        field: &Field,
        right_col: &ChunkedArray<Int8Array>,
        right_key_col: &ChunkedArray<UInt64Array>,
        result_key_col: &mut PrimitiveBuilder<UInt64Type>,
        bucket_size: usize,
        is_first: bool,
    ) -> Int8Array {
        // use buffer because standard is slower
        let mut res_builder = Int8Builder::with_capacity(bucket_size);
        let mut left_idx: usize = 0;
        let mut right_idx: usize = 0;

        let mut value = MergeValue::<i8>::new_null();
        while left_idx <= self.ops.len() - 1 && right_idx <= right_key_col.len() - 1 {
            value.reset();
            let left_key = self.ops[left_idx].0;
            let right_key = right_key_col.value(right_idx);
            let mut final_key: u64 = 0;
            match left_key.cmp(&right_key) {
                Ordering::Less => {
                    apply_right_i8_value(&self.ops, left_key, &mut left_idx, &mut value);
                    final_key = left_key;
                    left_idx += 1;
                }
                Ordering::Equal => {
                    value.is_null = right_col.is_null(right_idx);
                    value.value = right_col.value(right_idx);
                    apply_right_i8_value(&self.ops, left_key, &mut left_idx, &mut value);

                    final_key = left_key;
                    left_idx += 1;
                    right_idx += 1;
                }
                Ordering::Greater => {
                    value.is_null = right_col.is_null(right_idx);
                    value.value = right_col.value(right_idx);
                    final_key = right_key;
                    right_idx += 1;
                }
            }

            if value.is_set {
                if value.is_null {
                    res_builder.append_null();
                } else {
                    res_builder.append_value(value.value.clone());
                }
                if is_first {
                    result_key_col.append_value(final_key);
                }
            }
        }

        while left_idx <= self.ops.len() - 1 {
            let key = self.ops[left_idx].0;
            apply_right_i8_value(&self.ops, key, &mut left_idx, &mut value);
            left_idx += 1;
            if value.is_set {
                res_builder.append_null();
                if is_first {
                    result_key_col.append_value(key);
                }
            }
        }

        while right_idx <= right_col.len() - 1 {
            value.is_null = right_col.is_null(right_idx);
            value.value = right_col.value(right_idx);
            right_idx += 1;
            if value.is_set {
                if value.is_null {
                    res_builder.append_null();
                } else {
                    res_builder.append_value(value.value.clone());
                }
                if is_first {
                    result_key_col.append_value(right_key_col.value(right_idx));
                }
            }
        }

        res_builder.finish()
    }

    pub fn merge(&self, right: &[RecordBatch], bucket_size: usize) -> Result<RecordBatch> {
        let mut key_col = UInt64Array::builder(bucket_size);

        let left_schema = self.schema.clone();
        let right_schema = Schema::new(
            right[0]
                .schema()
                .fields()
                .iter()
                .map(|f| f.clone())
                .collect::<Vec<_>>(),
        );

        // merge bucket schema with batch (right) schema
        let schema = Schema::try_merge(vec![right_schema.clone(), self.schema.clone()]).unwrap();

        let right_key_chunks: Vec<&UInt64Array> = right
            .iter()
            .map(|x| x.column(0).as_any().downcast_ref::<UInt64Array>().unwrap())
            .collect();
        let right_key_col = ChunkedArray::new(right_key_chunks);

        // iterate over each column
        let mut cols: Vec<ArrayRef> = schema
            .fields()
            .iter()
            .skip(1)
            .enumerate()
            .map(|(col_idx, field)| {
                let left_col_id = left_schema
                    .column_with_name(field.name())
                    .map(|(col_id, _)| col_id);
                let right_col_id = right_schema
                    .column_with_name(field.name())
                    .map(|(col_id, _)| col_id);

                match field.data_type() {
                    DataType::Int8 => {
                        match (left_col_id, right_col_id) {
                            (Some(left_col_id), Some(right_col_id)) => {
                                let right_chunks: Vec<&Int8Array> = right
                                    .iter()
                                    .map(|x| {
                                        // we use col_id safely because it assumes that batch schema is less or equal to bucket schema
                                        x.column(right_col_id)
                                            .as_any()
                                            .downcast_ref::<Int8Array>()
                                            .unwrap()
                                    })
                                    .collect();
                                let right_col = ChunkedArray::new(right_chunks);
                                let res = self.merge_i8_columns(
                                    field,
                                    left_col_id,
                                    &right_col,
                                    &right_key_col,
                                    &mut key_col,
                                    bucket_size,
                                    col_idx == 0,
                                );

                                Arc::new(res) as ArrayRef
                            }
                            (Some(left_col_id), None) => {
                                let res = self.apply_left_i8_column(
                                    field,
                                    left_col_id,
                                    &right_key_col,
                                    &mut key_col,
                                    bucket_size,
                                    col_idx == 0,
                                );

                                Arc::new(res) as ArrayRef
                            }
                            (None, Some(right_col_id)) => {
                                let right_chunks: Vec<&Int8Array> = right
                                    .iter()
                                    .map(|x| {
                                        // we use col_id safely because it assumes that batch schema is less or equal to bucket schema
                                        x.column(right_col_id)
                                            .as_any()
                                            .downcast_ref::<Int8Array>()
                                            .unwrap()
                                    })
                                    .collect();
                                let right_col = ChunkedArray::new(right_chunks);
                                let res = self.apply_right_i8_column(
                                    field,
                                    &right_col,
                                    &right_key_col,
                                    &mut key_col,
                                    bucket_size,
                                    col_idx == 0,
                                );

                                Arc::new(res) as ArrayRef
                            }
                            (None, None) => unimplemented!(),
                        }
                    }
                    _ => unimplemented!(),
                }
            })
            .collect();

        let mut final_cols = vec![Arc::new(key_col.finish()) as ArrayRef];
        final_cols.append(&mut cols);
        Ok(RecordBatch::try_new(Arc::new(schema), final_cols).unwrap())
    }
}

fn merge_i8_value(
    ops: &[(u64, Op)],
    key: u64,
    idx: &mut usize,
    value: &mut MergeValue<i8>,
    col_id: usize,
) {
    while *idx <= ops.len() - 1 {
        let (_, op) = &ops[*idx];
        match op {
            Op::PutValues(vals) => {
                if col_id <= vals.len() - 1 && vals[col_id].is_some() {
                    if let ScalarValue::Int8(v) = vals[col_id].clone().unwrap() {
                        value.set_option(v.clone());
                    }
                }
            }
            Op::IncrementValue {
                col_id: inc_col_id,
                delta,
            } => {
                if col_id == *inc_col_id {
                    if let ScalarValue::Int8(v) = delta {
                        value.inc(v.unwrap());
                    }
                }
            }
            Op::DecrementValue {
                col_id: inc_col_id,
                delta,
            } => {
                if col_id == *inc_col_id {
                    if let ScalarValue::Int8(v) = delta {
                        value.dec(v.unwrap());
                    }
                }
            }
            Op::DeleteKey => {
                value.unset();
            }
            Op::None => {}
        }

        if *idx + 1 > ops.len() - 1 {
            return;
        }
        if ops[*idx + 1].0 != key {
            return;
        }
        *idx += 1;
    }
}

fn apply_right_i8_value(ops: &[(u64, Op)], key: u64, idx: &mut usize, value: &mut MergeValue<i8>) {
    while *idx <= ops.len() - 1 {
        let (_, op) = &ops[*idx];
        match op {
            Op::DeleteKey => {
                value.unset();
            }
            _ => {}
        }

        if *idx + 1 > ops.len() - 1 {
            return;
        }
        if ops[*idx + 1].0 != key {
            return;
        }
        *idx += 1;
    }
}

#[derive(Clone)]
struct Bucket {
    id: usize,
    min_key: u64,
    max_key: u64,
}

impl Bucket {
    pub fn new(id: usize, min_key: u64, max_key: u64) -> Self {
        Bucket {
            id,
            min_key,
            max_key,
        }
    }
}

pub struct Storage {
    schema: Schema,
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

struct MergeValue<T: Clone> {
    is_set: bool,
    is_null: bool,
    value: T,
}

impl<T: Default + Clone> MergeValue<T> {
    pub fn new_null() -> Self {
        MergeValue {
            is_set: true,
            is_null: true,
            value: T::default(),
        }
    }
    pub fn reset(&mut self) {
        self.is_set = true;
        self.is_null = true;
        self.value = T::default();
    }
    pub fn unset(&mut self) {
        self.is_set = false;
        self.is_null = false;
    }

    pub fn set_option(&mut self, value: Option<T>) {
        self.is_set = true;
        match value {
            None => self.is_null = true,
            Some(v) => {
                self.is_null = false;
                self.value = v;
            }
        }
    }

    pub fn set(&mut self, value: T) {
        self.is_set = true;
        self.value = value;
        self.is_null = false;
    }

    pub fn make_null(&mut self) {
        self.is_null = true;
    }

    pub fn inc(&mut self, delta: T)
    where T: AddAssign {
        self.is_set = true;
        self.is_null = false;
        self.value += delta;
    }

    pub fn dec(&mut self, delta: T)
    where T: SubAssign {
        self.is_set = true;
        self.is_null = false;
        self.value -= delta;
    }
}

trait ChunkIndex<V> {
    fn value(&self, i: usize) -> V;
    fn is_null(&self, i: usize) -> bool;
    fn len(&self) -> usize;
}

struct ChunkedArray<'a, T> {
    chunks: Vec<&'a T>,
    len: usize,
}

impl<'a, T: Array> ChunkedArray<'a, T> {
    fn new(chunks: Vec<&'a T>) -> Self {
        Self {
            chunks: chunks.clone(),
            len: chunks.iter().fold(0, |acc, &x| acc + x.len()),
        }
    }
}

impl<'a> ChunkIndex<i8> for ChunkedArray<'a, Int8Array> {
    fn value(&self, mut i: usize) -> i8 {
        for chunk in self.chunks.iter() {
            if i <= chunk.len() - 1 {
                return chunk.value(i);
            }
            i -= chunk.len();
        }
        unreachable!();
    }

    fn is_null(&self, mut i: usize) -> bool {
        for chunk in self.chunks.iter() {
            if i <= chunk.len() - 1 {
                return chunk.is_null(i);
            }
            i -= chunk.len();
        }
        unreachable!();
    }

    fn len(&self) -> usize {
        self.len
    }
}

impl<'a> ChunkIndex<u64> for ChunkedArray<'a, UInt64Array> {
    fn value(&self, mut i: usize) -> u64 {
        for chunk in self.chunks.iter() {
            if i <= chunk.len() - 1 {
                return chunk.value(i);
            }
            i -= chunk.len();
        }
        unreachable!();
    }

    fn is_null(&self, mut i: usize) -> bool {
        for chunk in self.chunks.iter() {
            if i <= chunk.len() - 1 {
                return chunk.is_null(i);
            }
            i -= chunk.len();
        }
        unreachable!();
    }

    fn len(&self) -> usize {
        self.len
    }
}

enum Merge {
    TakeLeft,
    TakeRight,
    Merge,
}

impl Storage {
    pub fn put_values(&mut self, key: u64, values: Vec<Value>) -> Result<()> {
        let mut res: Vec<Option<ScalarValue>> = vec![None; self.schema.fields().len()];
        for v in values.iter() {
            res[v.col_id] = Some(v.value.clone());
        }
        self.insert_op(key, Op::PutValues(res))
    }

    fn insert_op(&mut self, key: u64, op: Op) -> Result<()> {
        self.memory.insert_op(key, op)?;

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
            self.merge_ops_bucket_sender
                .send(MergeOpsBucket::new(bucket_id, bucket.clone()));
        }
        Ok(())
    }

    fn split_rows_by_buckets(
        &self,
        ordered_ops: &skiplist::OrderedSkipList<OrderedOp>,
    ) -> Result<Vec<Arc<OpsBucket>>> {
        let mut ret: Vec<Arc<OpsBucket>> = Vec::new();
        let mut ops = vec![(0u64, Op::None); self.bucket_size];
        let mut iter = ordered_ops.iter();
        let mut bucket_index: usize = 0;
        // bucket vars
        let mut keys_count: usize = 0;
        let mut lat_key: u64 = 0;
        let mut min_key: u64 = u64::MAX;
        let mut max_key: u64 = 0;
        let mut end = false;
        ops.truncate(0);

        for op in ordered_ops.iter() {
            if lat_key != op.key {
                keys_count += 1;
            }
            if op.key < min_key {
                min_key = op.key;
            }
            if op.key > max_key {
                max_key = op.key;
            }

            if bucket_index != op.key as usize % self.bucket_size {
                if !ops.is_empty() {
                    let ops_bucket = OpsBucket {
                        idx: bucket_index,
                        schema: self.schema.clone(),
                        keys_count,
                        min_key,
                        max_key,
                        ops: ops.clone(),
                    };
                    ret.push(Arc::new(ops_bucket));
                    ops.truncate(0);
                    keys_count = 0;
                    min_key = u64::MAX;
                    max_key = 0;
                }
                bucket_index = op.key as usize % self.bucket_size;
            }
            ops.push(op.keyed_op());
        }

        if !ops.is_empty() {
            let ops_bucket = OpsBucket {
                idx: bucket_index,
                schema: self.schema.clone(),
                keys_count,
                min_key,
                max_key,
                ops: ops.clone(),
            };
            ret.push(Arc::new(ops_bucket));
            ops.truncate(0);
        }

        Ok(ret)
    }

    fn merge_ops_bucket(&mut self, merge: MergeOpsBucket) -> Result<()> {
        let mut cur_data = RecordBatch::new_empty(Arc::new(self.schema.clone()));
        match self.get_bucket_by_index(merge.ops_bucket.idx)? {
            None => self.write_ops_bucket(merge.ops_bucket.clone())?,
            Some(bucket) => {
                self.merge_ops_bucket_with_existing(bucket, merge.ops_bucket.clone())?;
            }
        }
        Ok(())
    }

    fn merge_ops_bucket_with_existing(
        &mut self,
        bucket: Bucket,
        ops_bucket: Arc<OpsBucket>,
    ) -> Result<()> {
        let in_file = File::open("parquet.file").unwrap();
        //let in_reader = SerializedFileReader::new(in_file).unwrap();
        let mut arrow_reader = ParquetRecordBatchReader::try_new(in_file, 2048).unwrap();
        //let mut record_batch_reader = arrow_reader.get_record_reader(2048).unwrap();

        let out_file = File::create("parquet.file").unwrap();
        let schema_ref = SchemaRef::new(ops_bucket.schema.clone());
        let mut rb_writer =
            ArrowWriter::try_new(out_file, schema_ref, None).unwrap();
        for b in arrow_reader {
            let batch = b.unwrap();
        }
        Ok(())
    }

    fn write_ops_bucket(&mut self, bucket: Arc<OpsBucket>) -> Result<()> {
        let out_file = File::create(Path::new(&bucket.idx.to_string())).unwrap();
        let mut rb_writer =
            ArrowWriter::try_new(out_file, Arc::new(bucket.schema.clone()), None).unwrap();

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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::ArrayRef;
    use arrow::array::Int8Builder;
    use arrow::array::UInt64Builder;
    use arrow::datatypes::DataType;
    use arrow::datatypes::Field;
    use arrow::datatypes::Schema;
    use arrow::record_batch::RecordBatch;
    use datafusion_common::ScalarValue;

    use crate::sorted_hash_map::storage::Op;
    use crate::sorted_hash_map::storage::OpsBucket;

    #[test]
    fn op_bucket_to_record_batch() {
        let mut ops: Vec<(u64, Op)> = Vec::new();
        ops.push((
            1,
            Op::PutValues(vec![None, Some(ScalarValue::Int8(Some(1)))]),
        ));
        ops.push((
            1,
            Op::PutValues(vec![None, Some(ScalarValue::Int8(Some(2)))]),
        ));
        ops.push((
            1,
            Op::PutValues(vec![
                Some(ScalarValue::Int8(Some(2))),
                Some(ScalarValue::Int8(Some(3))),
            ]),
        ));
        ops.push((
            1,
            Op::PutValues(vec![None, Some(ScalarValue::Int8(Some(1)))]),
        ));

        ops.push((2, Op::PutValues(vec![Some(ScalarValue::Int8(Some(1)))])));

        ops.push((
            3,
            Op::PutValues(vec![None, Some(ScalarValue::Int8(Some(2)))]),
        ));

        ops.push((4, Op::IncrementValue {
            col_id: 0,
            delta: ScalarValue::Int8(Some(1)),
        }));
        ops.push((4, Op::IncrementValue {
            col_id: 0,
            delta: ScalarValue::Int8(Some(3)),
        }));
        ops.push((4, Op::DecrementValue {
            col_id: 0,
            delta: ScalarValue::Int8(Some(1)),
        }));
        ops.push((4, Op::IncrementValue {
            col_id: 1,
            delta: ScalarValue::Int8(Some(1)),
        }));
        ops.push((4, Op::DeleteKey));

        ops.push((5, Op::IncrementValue {
            col_id: 0,
            delta: ScalarValue::Int8(Some(1)),
        }));
        ops.push((5, Op::IncrementValue {
            col_id: 0,
            delta: ScalarValue::Int8(Some(3)),
        }));
        ops.push((5, Op::DecrementValue {
            col_id: 0,
            delta: ScalarValue::Int8(Some(1)),
        }));
        ops.push((5, Op::IncrementValue {
            col_id: 1,
            delta: ScalarValue::Int8(Some(1)),
        }));

        let schema = Schema::new(vec![
            Field::new("a", DataType::Int8, false),
            Field::new("b", DataType::Int8, false),
        ]);

        let mut bucket = OpsBucket {
            idx: 0,
            schema: schema.clone(),
            keys_count: 0,
            min_key: 0,
            max_key: 0,
            ops: ops.clone(),
        };

        let rb = bucket.to_record_batch().unwrap();

        let exp = vec![
            "+-----+---+---+",
            "| key | a | b |",
            "+-----+---+---+",
            "| 1   | 2 | 1 |",
            "| 2   | 1 |   |",
            "| 3   |   | 2 |",
            "| 5   | 3 | 1 |",
            "+-----+---+---+\n",
        ];

        // TODO: fix assert
        //assert_eq!(
        //    exp.join("\n"),
        //    arrow::util::pretty::pretty_format_batches(&[rb.clone()]).unwrap(),
        //);
    }

    fn build_columns(vals: &[ScalarValue], cols: usize) -> Vec<ArrayRef> {
        let len = vals.len() / cols;
        (0..cols)
            .into_iter()
            .map(|col_id| match vals[col_id].get_datatype() {
                arrow_schema::DataType::UInt64 => {
                    let mut arr = UInt64Builder::with_capacity(len);
                    vals.iter().skip(col_id).step_by(cols).for_each(|sv| {
                        if let ScalarValue::UInt64(v) = sv {
                            arr.append_option(v.clone());
                        }
                    });
                    Arc::new(arr.finish()) as ArrayRef
                }
                arrow_schema::DataType::Int8 => {
                    let mut arr = Int8Builder::with_capacity(len);
                    vals.iter().skip(col_id).step_by(cols).for_each(|sv| {
                        if let ScalarValue::Int8(v) = sv {
                            arr.append_option(v.clone());
                        }
                    });
                    Arc::new(arr.finish()) as ArrayRef
                }
                _ => unimplemented!(),
            })
            .collect()
    }

    #[test]
    fn merge() {
        let mut ops: Vec<(u64, Op)> = Vec::new();
        ops.push((
            1,
            Op::PutValues(vec![None, Some(ScalarValue::Int8(Some(1)))]),
        ));
        ops.push((
            1,
            Op::PutValues(vec![None, Some(ScalarValue::Int8(Some(2)))]),
        ));
        ops.push((
            1,
            Op::PutValues(vec![
                Some(ScalarValue::Int8(Some(2))),
                Some(ScalarValue::Int8(Some(3))),
            ]),
        ));
        ops.push((
            1,
            Op::PutValues(vec![None, Some(ScalarValue::Int8(Some(1)))]),
        ));

        ops.push((2, Op::PutValues(vec![Some(ScalarValue::Int8(Some(1)))])));

        ops.push((
            3,
            Op::PutValues(vec![None, Some(ScalarValue::Int8(Some(2)))]),
        ));

        ops.push((4, Op::IncrementValue {
            col_id: 0,
            delta: ScalarValue::Int8(Some(1)),
        }));
        ops.push((4, Op::IncrementValue {
            col_id: 0,
            delta: ScalarValue::Int8(Some(3)),
        }));
        ops.push((4, Op::DecrementValue {
            col_id: 0,
            delta: ScalarValue::Int8(Some(1)),
        }));
        ops.push((4, Op::IncrementValue {
            col_id: 1,
            delta: ScalarValue::Int8(Some(1)),
        }));
        ops.push((4, Op::DeleteKey));

        ops.push((5, Op::IncrementValue {
            col_id: 0,
            delta: ScalarValue::Int8(Some(1)),
        }));
        ops.push((5, Op::IncrementValue {
            col_id: 0,
            delta: ScalarValue::Int8(Some(3)),
        }));
        ops.push((5, Op::DecrementValue {
            col_id: 0,
            delta: ScalarValue::Int8(Some(1)),
        }));
        ops.push((5, Op::IncrementValue {
            col_id: 1,
            delta: ScalarValue::Int8(Some(1)),
        }));

        ops.push((7, Op::IncrementValue {
            col_id: 1,
            delta: ScalarValue::Int8(Some(1)),
        }));

        let schema = Schema::new(vec![
            Field::new("a", DataType::Int8, true),
            Field::new("b", DataType::Int8, true),
        ]);

        let mut bucket = OpsBucket {
            idx: 0,
            schema: schema.clone(),
            keys_count: 0,
            min_key: 0,
            max_key: 0,
            ops: ops.clone(),
        };

        let batch_schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::UInt64, false),
            Field::new("a", DataType::Int8, true),
            Field::new("c", DataType::Int8, true),
        ]));

        let batches = {
            let cols1 = vec![
                //           key                           a                           c
                ScalarValue::UInt64(Some(1)),
                ScalarValue::Int8(Some(1)),
                ScalarValue::Int8(Some(3)),
                ScalarValue::UInt64(Some(2)),
                ScalarValue::Int8(None),
                ScalarValue::Int8(Some(4)),
                ScalarValue::UInt64(Some(3)),
                ScalarValue::Int8(Some(1)),
                ScalarValue::Int8(Some(3)),
            ];

            let batch1 =
                RecordBatch::try_new(batch_schema.clone(), build_columns(&cols1, 3)).unwrap();

            let cols2 = vec![
                //           key                           a                           c
                ScalarValue::UInt64(Some(4)),
                ScalarValue::Int8(Some(1)),
                ScalarValue::Int8(Some(1)),
                ScalarValue::UInt64(Some(5)),
                ScalarValue::Int8(Some(5)),
                ScalarValue::Int8(Some(3)),
                ScalarValue::UInt64(Some(6)),
                ScalarValue::Int8(Some(6)),
                ScalarValue::Int8(Some(6)),
            ];

            let batch2 =
                RecordBatch::try_new(batch_schema.clone(), build_columns(&cols2, 3)).unwrap();

            vec![batch1, batch2]
        };

        let rb = bucket.merge(&batches, 100).unwrap();
        let exp = vec![
            "+-----+---+---+---+",
            "| key | a | c | b |",
            "+-----+---+---+---+",
            "| 1   | 2 | 3 | 1 |",
            "| 2   | 1 | 4 |   |",
            "| 3   | 1 | 3 | 2 |",
            "| 5   | 8 | 3 | 1 |",
            "| 6   | 6 | 6 |   |",
            "| 7   |   |   | 1 |",
            "+-----+---+---+---+\n",
        ];

        //TODO: fix assert
        //assert_eq!(
        //    exp.join("\n"),
        //    arrow::util::pretty::pretty_format_batches(&[rb.clone()]).unwrap(),
        //);
    }
}

