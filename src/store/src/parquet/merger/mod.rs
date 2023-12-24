//! Contains merger which merges multiple parquets into one.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::fs;
use std::fs::File;
use std::io::BufReader;
use std::io::Read;
use std::io::Seek;
use std::io::Write;
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::path::PathBuf;

use arrow2::array::new_null_array;
use arrow2::array::Array;
use arrow2::array::BinaryArray;
use arrow2::array::BooleanArray;
use arrow2::array::Float32Array;
use arrow2::array::Float64Array;
use arrow2::array::Int128Array;
use arrow2::array::Int16Array;
use arrow2::array::Int32Array;
use arrow2::array::Int64Array;
use arrow2::array::Int8Array;
use arrow2::array::ListArray;
use arrow2::array::MutableArray;
use arrow2::array::MutableBinaryArray;
use arrow2::array::MutableBooleanArray;
use arrow2::array::MutableListArray;
use arrow2::array::MutablePrimitiveArray;
use arrow2::array::MutableUtf8Array;
use arrow2::array::TryPush;
use arrow2::array::UInt16Array;
use arrow2::array::UInt32Array;
use arrow2::array::UInt64Array;
use arrow2::array::UInt8Array;
use arrow2::array::Utf8Array;
use arrow2::bitmap::Bitmap;
use arrow2::chunk::Chunk;
use arrow2::datatypes::DataType;
use arrow2::datatypes::Field;
use arrow2::datatypes::PhysicalType as ArrowPhysicalType;
use arrow2::datatypes::PrimitiveType as ArrowPrimitiveType;
use arrow2::datatypes::Schema;
use arrow2::io::parquet::read::infer_schema;
use arrow2::io::parquet::write::add_arrow_schema;
use arrow2::io::parquet::write::to_parquet_schema;
use arrow2::offset::OffsetsBuffer;
use parquet2::metadata::ColumnDescriptor;
use parquet2::metadata::SchemaDescriptor;
use parquet2::page::CompressedPage;
use parquet2::write::FileSeqWriter;
use parquet2::write::Version;
use parquet2::write::WriteOptions;

use crate::error::Result;
use crate::error::StoreError;
use crate::merge_arrays;
use crate::merge_arrays_inner;
use crate::merge_list_arrays;
use crate::merge_list_arrays_inner;
use crate::merge_list_primitive_arrays;
use crate::merge_primitive_arrays;
use crate::parquet::merger::parquet::array_to_pages_simple;
use crate::parquet::merger::parquet::data_page_to_array;
use crate::parquet::merger::parquet::ColumnPath;
use crate::parquet::merger::parquet::CompressedPageIterator;
use crate::parquet::merger::parquet::ParquetValue;
use crate::KeyValue;
use crate::Value;

pub mod arrow_merger;
mod merge_data_arrays;
pub mod parquet;
pub mod parquet_merger;

pub fn chunk_min_max(
    chunk: &Chunk<Box<dyn Array>>,
    index_cols: usize,
) -> (Vec<KeyValue>, Vec<KeyValue>) {
    let (min_values, max_values) = (0..index_cols)
        .into_iter()
        .map(|id| {
            let arr = chunk.columns()[id].as_ref();
            match arr.data_type() {
                // todo move to macro
                DataType::Int8 => {
                    let arr = arr.as_any().downcast_ref::<Int8Array>().unwrap();
                    let min = KeyValue::Int8(arr.value(0));
                    let max = KeyValue::Int8(arr.value(arr.len() - 1));
                    (min, max)
                }
                DataType::Int16 => {
                    let arr = arr.as_any().downcast_ref::<Int16Array>().unwrap();
                    let min = KeyValue::Int16(arr.value(0));
                    let max = KeyValue::Int16(arr.value(arr.len() - 1));
                    (min, max)
                }
                DataType::Int32 => {
                    let arr = arr.as_any().downcast_ref::<Int32Array>().unwrap();
                    let min = KeyValue::Int32(arr.value(0));
                    let max = KeyValue::Int32(arr.value(arr.len() - 1));
                    (min, max)
                }
                DataType::Int64 => {
                    let arr = arr.as_any().downcast_ref::<Int64Array>().unwrap();
                    let min = KeyValue::Int64(arr.value(0));
                    let max = KeyValue::Int64(arr.value(arr.len() - 1));
                    (min, max)
                }
                DataType::Timestamp(_, _) => {
                    let arr = arr.as_any().downcast_ref::<Int64Array>().unwrap();
                    let min = KeyValue::Timestamp(arr.value(0));
                    let max = KeyValue::Timestamp(arr.value(arr.len() - 1));
                    (min, max)
                }
                _ => unimplemented!("unsupported type {:?}", arr.data_type()),
            }
        })
        .unzip();

    (min_values, max_values)
}

pub trait IndexChunk {
    fn min_values(&self) -> Vec<ParquetValue>;
    fn max_values(&self) -> Vec<ParquetValue>;
}

// Merge arrow2 schemas
pub fn try_merge_arrow_schemas(schemas: Vec<Schema>) -> Result<Schema> {
    let fields: Result<Vec<Field>> = schemas.into_iter().map(|schema| schema.fields).try_fold(
        Vec::<Field>::new(),
        |mut merged, unmerged| {
            for field in unmerged.into_iter() {
                let merged_field = merged
                    .iter_mut()
                    .find(|merged_field| merged_field.name == field.name);
                match merged_field {
                    None => merged.push(field.to_owned()),
                    Some(merged_field) => {
                        if *merged_field != field {
                            return Err(StoreError::InvalidParameter(format!(
                                "Fields are not equal: {:?} {:?}",
                                merged_field, field
                            )));
                        }
                    }
                }
            }

            Ok(merged)
        },
    );

    Ok(Schema::from(fields?))
}

// Row for merge chunks with one column partition

// usize - stream id
// A - partition type
pub struct OneColMergeRow<A>(usize, A);

impl<A> Ord for OneColMergeRow<A>
where A: Ord
{
    fn cmp(&self, other: &Self) -> Ordering {
        self.1.cmp(&other.1).reverse()
    }
}

impl<A> PartialOrd for OneColMergeRow<A>
where A: Ord
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<A> PartialEq for OneColMergeRow<A>
where A: Eq
{
    fn eq(&self, other: &Self) -> bool {
        self.1 == other.1
    }
}

impl<A> Eq for OneColMergeRow<A> where A: Eq {}

// Row for merge chunks with two columns partition

// usize - stream id
// A,B - partition types
pub struct TwoColMergeRow<A, B>(usize, A, B);

impl<A, B> Ord for TwoColMergeRow<A, B>
where
    A: Ord,
    B: Ord,
{
    fn cmp(&self, other: &Self) -> Ordering {
        (&self.1, &self.2).cmp(&(&other.1, &other.2)).reverse()
    }
}

impl<A, B> PartialOrd for TwoColMergeRow<A, B>
where
    A: Ord,
    B: Ord,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<A, B> PartialEq for TwoColMergeRow<A, B>
where
    A: Eq,
    B: Eq,
{
    fn eq(&self, other: &Self) -> bool {
        (&self.1, &self.2) == (&other.1, &other.2)
    }
}

impl<A, B> Eq for TwoColMergeRow<A, B>
where
    A: Eq,
    B: Eq,
{
}

pub struct ThreeColMergeRow<A, B, C>(usize, A, B, C);

impl<A, B, C> Ord for crate::parquet::merger::ThreeColMergeRow<A, B, C>
where
    A: Ord,
    B: Ord,
    C: Ord,
{
    fn cmp(&self, other: &Self) -> Ordering {
        (&self.1, &self.2, &self.3)
            .cmp(&(&other.1, &other.2, &other.3))
            .reverse()
    }
}

impl<A, B, C> PartialOrd for crate::parquet::merger::ThreeColMergeRow<A, B, C>
where
    A: Ord,
    B: Ord,
    C: Ord,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<A, B, C> PartialEq for crate::parquet::merger::ThreeColMergeRow<A, B, C>
where
    A: Eq,
    B: Eq,
    C: Eq,
{
    fn eq(&self, other: &Self) -> bool {
        (&self.1, &self.2, &self.3) == (&other.1, &other.2, &other.3)
    }
}

impl<A, B, C> Eq for crate::parquet::merger::ThreeColMergeRow<A, B, C>
where
    A: Eq,
    B: Eq,
    C: Eq,
{
}
// this is a temporary array used to merge data pages avoiding downcasting

enum TmpArray {
    Int8(Int8Array),
    Int16(Int16Array),
    Int32(Int32Array),
    Int64(Int64Array),
    Int128(Int128Array),
    UInt8(UInt8Array),
    UInt16(UInt16Array),
    UInt32(UInt32Array),
    UInt64(UInt64Array),
    Float32(Float32Array),
    Float64(Float64Array),
    Boolean(BooleanArray),
    // FixedSizeBinary(FixedSizeBinaryArray),
    Binary(BinaryArray<i32>),
    LargeBinary(BinaryArray<i64>),
    Utf8(Utf8Array<i32>),
    LargeUtf8(Utf8Array<i64>),
    ListInt8(Int8Array, OffsetsBuffer<i32>, Option<Bitmap>, usize),
    ListInt16(Int16Array, OffsetsBuffer<i32>, Option<Bitmap>, usize),
    ListInt32(Int32Array, OffsetsBuffer<i32>, Option<Bitmap>, usize),
    ListInt64(Int64Array, OffsetsBuffer<i32>, Option<Bitmap>, usize),
    ListInt128(Int128Array, OffsetsBuffer<i32>, Option<Bitmap>, usize),
    ListUInt8(UInt8Array, OffsetsBuffer<i32>, Option<Bitmap>, usize),
    ListUInt16(UInt16Array, OffsetsBuffer<i32>, Option<Bitmap>, usize),
    ListUInt32(UInt32Array, OffsetsBuffer<i32>, Option<Bitmap>, usize),
    ListUInt64(UInt64Array, OffsetsBuffer<i32>, Option<Bitmap>, usize),
    ListFloat32(Float32Array, OffsetsBuffer<i32>, Option<Bitmap>, usize),
    ListFloat64(Float64Array, OffsetsBuffer<i32>, Option<Bitmap>, usize),
    ListBoolean(BooleanArray, OffsetsBuffer<i32>, Option<Bitmap>, usize),
    // ListFixedSizeBinary(
    // FixedSizeBinaryArray,
    // OffsetsBuffer<i32>,
    // Option<Bitmap>,
    // usize,
    // ),
    ListBinary(BinaryArray<i32>, OffsetsBuffer<i32>, Option<Bitmap>, usize),
    ListLargeBinary(BinaryArray<i64>, OffsetsBuffer<i32>, Option<Bitmap>, usize),
    ListUtf8(Utf8Array<i32>, OffsetsBuffer<i32>, Option<Bitmap>, usize),
    ListLargeUtf8(Utf8Array<i64>, OffsetsBuffer<i32>, Option<Bitmap>, usize),
}
