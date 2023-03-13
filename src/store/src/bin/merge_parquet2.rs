use std::cmp::Ordering;
use std::collections::{BinaryHeap, VecDeque};
use std::io::Read;
use std::io::Write;
use std::marker::PhantomData;

use arrow2::array::Array;
use arrow2::array::PrimitiveArray;
use parquet2::metadata::ColumnDescriptor;
use parquet2::metadata::SchemaDescriptor;
use parquet2::page::{CompressedDataPage, CompressedPage};
use parquet2::read::{PageIterator, PageReader};
use parquet2::write::{Compressor, FileSeqWriter, Version};
use rayon::prelude::*;
use arrow2::datatypes::{DataType, PrimitiveType, TimeUnit};
use arrow2::io::parquet::read::deserialize::page_iter_to_arrays;
use parquet2::schema::types::{ParquetType, PhysicalType};
use parquet2::statistics::{BinaryStatistics, BooleanStatistics, FixedLenStatistics, PrimitiveStatistics};
use store::error::{Result, StoreError};
use arrow2::io::parquet::read::decompress;
use arrow2::io::parquet::write::{array_to_columns, array_to_page_simple, WriteOptions};
use parquet2::compression::CompressionOptions;
use parquet2::encoding::Encoding;


#[derive(Debug)]
struct ReorderSlices {}

#[derive(Eq, PartialEq, PartialOrd, Ord, Debug, Clone, Copy)]
enum OptionalValue {
    Boolean(Option<bool>),
    Int32(Option<i32>),
    Int64(Option<i64>),
    Float(Option<f64>),
    Double(Option<f64>),
    ByteArray(Option<Vec<u8>>),
}














fn main() -> anyhow::Result<()> {
    todo!()
}
