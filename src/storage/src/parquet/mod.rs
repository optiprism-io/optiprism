//! Contains merger which merges multiple parquets into one.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::fs::File;
use std::io::BufReader;
use std::io::Read;
use std::io::Seek;

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
use arrow2::array::UInt16Array;
use arrow2::array::UInt32Array;
use arrow2::array::UInt64Array;
use arrow2::array::UInt8Array;
use arrow2::array::Utf8Array;
use arrow2::bitmap::Bitmap;
use arrow2::chunk::Chunk;
use arrow2::datatypes::DataType;
use arrow2::datatypes::Field;
use arrow2::datatypes::Schema;
use arrow2::io;
use arrow2::io::parquet::read::column_iter_to_arrays;
use arrow2::io::parquet::read::deserialize::page_iter_to_arrays;
use arrow2::io::parquet::read::schema::convert::to_primitive_type;
use arrow2::io::parquet::read::ParquetError;
use arrow2::io::parquet::write::array_to_columns;
use arrow2::io::parquet::write::WriteOptions;
use arrow2::offset::OffsetsBuffer;
use ordered_float::OrderedFloat;
use parquet2::compression::CompressionOptions;
use parquet2::compression::ZstdLevel;
use parquet2::encoding::Encoding;
use parquet2::metadata::ColumnDescriptor;
use parquet2::metadata::FileMetaData;
use parquet2::page::CompressedDataPage;
use parquet2::page::CompressedPage;
use parquet2::read::decompress;
use parquet2::schema::types::ParquetType;
use parquet2::schema::types::PhysicalType;
use parquet2::statistics::BinaryStatistics;
use parquet2::statistics::FixedLenStatistics;
use parquet2::statistics::PrimitiveStatistics;
use parquet2::write::compress;
use parquet2::write::Version;

use crate::error::Result;
use crate::error::StoreError;
use crate::KeyValue;

pub mod arrow_merger;
mod merge_data_arrays;
pub mod parquet_merger;

pub fn chunk_min_max(
    chunk: &Chunk<Box<dyn Array>>,
    index_cols: usize,
) -> (Vec<KeyValue>, Vec<KeyValue>) {
    let (min_values, max_values) = (0..index_cols)
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
#[derive(Debug)]
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

impl<A, B, C> Ord for crate::parquet::ThreeColMergeRow<A, B, C>
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

impl<A, B, C> PartialOrd for crate::parquet::ThreeColMergeRow<A, B, C>
where
    A: Ord,
    B: Ord,
    C: Ord,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<A, B, C> PartialEq for crate::parquet::ThreeColMergeRow<A, B, C>
where
    A: Eq,
    B: Eq,
    C: Eq,
{
    fn eq(&self, other: &Self) -> bool {
        (&self.1, &self.2, &self.3) == (&other.1, &other.2, &other.3)
    }
}

impl<A, B, C> Eq for crate::parquet::ThreeColMergeRow<A, B, C>
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

#[derive(Eq, PartialEq, PartialOrd, Ord, Debug, Clone)]
pub enum ParquetValue {
    Boolean(bool),
    Int32(i32),
    Int64(i64),
    Int96([u32; 3]),
    Float(OrderedFloat<f32>),
    Double(OrderedFloat<f64>),
    ByteArray(Vec<u8>),
}

impl From<bool> for ParquetValue {
    fn from(value: bool) -> Self {
        ParquetValue::Boolean(value)
    }
}

impl From<i8> for ParquetValue {
    fn from(value: i8) -> Self {
        ParquetValue::Int32(value as i32)
    }
}

impl From<i16> for ParquetValue {
    fn from(value: i16) -> Self {
        ParquetValue::Int32(value as i32)
    }
}

impl From<i32> for ParquetValue {
    fn from(value: i32) -> Self {
        ParquetValue::Int32(value)
    }
}

impl From<i64> for ParquetValue {
    fn from(value: i64) -> Self {
        ParquetValue::Int64(value)
    }
}

impl From<[u32; 3]> for ParquetValue {
    fn from(value: [u32; 3]) -> Self {
        ParquetValue::Int96(value)
    }
}

impl From<f32> for ParquetValue {
    fn from(value: f32) -> Self {
        ParquetValue::Float(OrderedFloat(value))
    }
}

impl From<f64> for ParquetValue {
    fn from(value: f64) -> Self {
        ParquetValue::Double(OrderedFloat(value))
    }
}

impl From<Vec<u8>> for ParquetValue {
    fn from(value: Vec<u8>) -> Self {
        ParquetValue::ByteArray(value)
    }
}

macro_rules! min_max_values {
    ($first_stats:expr,$last_stats:expr,$ty:ty) => {{
        match (
            $first_stats.as_any().downcast_ref::<$ty>(),
            $last_stats.as_any().downcast_ref::<$ty>(),
        ) {
            (Some(first), Some(last)) => (
                ParquetValue::from(first.min_value.clone().unwrap()),
                ParquetValue::from(last.max_value.clone().unwrap()),
            ),
            _ => return Err(StoreError::Internal("no stats".to_string())),
        }
    }};
}

pub fn pages_to_arrays(
    pages: Vec<CompressedPage>,
    cd: &ColumnDescriptor,
    chunk_size: Option<usize>,
    buf: &mut Vec<u8>,
) -> Result<Vec<Box<dyn Array>>> {
    let data_type = to_primitive_type(&cd.descriptor.primitive_type);
    let num_rows = pages
        .iter()
        .map(|page| match page {
            CompressedPage::Data(page) => page.num_values(),
            _ => unimplemented!("dict page is not supported"),
        })
        .sum();

    let decompressed_pages = pages
        .into_iter()
        .map(|page| decompress(page, buf))
        .collect::<parquet2::error::Result<Vec<_>>>()?;

    let iter = decompressed_pages
        .iter()
        .map(Ok)
        .collect::<Vec<std::result::Result<_, ParquetError>>>();
    let fallible_pages = fallible_streaming_iterator::convert(iter.into_iter());
    let res = page_iter_to_arrays(
        fallible_pages,
        &cd.descriptor.primitive_type,
        data_type,
        chunk_size,
        num_rows,
    )?;
    Ok(res.collect::<arrow2::error::Result<Vec<_>>>()?)
}

pub fn data_page_to_array(
    page: CompressedDataPage,
    cd: &ColumnDescriptor,
    field: Field,
    buf: &mut Vec<u8>,
) -> Result<Box<dyn Array>> {
    let num_rows = page.num_values();
    let decompressed_page = decompress(CompressedPage::Data(page), buf)?;
    let iter = fallible_streaming_iterator::convert(std::iter::once(Ok(&decompressed_page)));
    let mut arrs = column_iter_to_arrays(
        vec![iter],
        vec![&cd.descriptor.primitive_type.clone()],
        field,
        None,
        num_rows,
    )?;
    Ok(arrs.next().unwrap()?)
}

pub fn array_to_pages_simple(
    arr: Box<dyn Array>,
    typ: ParquetType,
    data_pagesize_limit: Option<usize>,
) -> Result<Vec<CompressedPage>> {
    let opts = WriteOptions {
        write_statistics: true,
        compression: CompressionOptions::Zstd(Some(ZstdLevel::try_new(1).unwrap())), // todo
        version: Version::V2,
        data_pagesize_limit,
    };

    let pages = array_to_columns(arr, typ, opts, &[Encoding::Plain])?
        .pop()
        .unwrap();
    let compressed = pages
        .into_iter()
        .map(|page| {
            compress(
                page.unwrap(),
                vec![],
                CompressionOptions::Zstd(Some(ZstdLevel::try_new(1).unwrap())),
            )
        })
        .collect::<std::result::Result<Vec<CompressedPage>, _>>()?;

    Ok(compressed)
}

pub fn get_page_min_max_values(pages: &[CompressedPage]) -> Result<(ParquetValue, ParquetValue)> {
    match (pages.first().as_ref(), pages.last().as_ref()) {
        (Some(&CompressedPage::Data(first)), Some(&CompressedPage::Data(last))) => {
            match (first.statistics(), last.statistics()) {
                (Some(first_stats), Some(last_stats)) => {
                    let first_stats = first_stats?;
                    let last_stats = last_stats?;

                    let (min_value, max_value) = match first_stats.physical_type() {
                        PhysicalType::Int32 => {
                            min_max_values!(first_stats, last_stats, PrimitiveStatistics<i32>)
                        }
                        PhysicalType::Int64 => {
                            min_max_values!(first_stats, last_stats, PrimitiveStatistics<i64>)
                        }
                        PhysicalType::Int96 => {
                            min_max_values!(first_stats, last_stats, PrimitiveStatistics<[u32; 3]>)
                        }
                        PhysicalType::Float => {
                            min_max_values!(first_stats, last_stats, PrimitiveStatistics<f32>)
                        }
                        PhysicalType::Double => {
                            min_max_values!(first_stats, last_stats, PrimitiveStatistics<f64>)
                        }
                        PhysicalType::ByteArray => {
                            min_max_values!(first_stats, last_stats, BinaryStatistics)
                        }
                        PhysicalType::FixedLenByteArray(_) => {
                            min_max_values!(first_stats, last_stats, FixedLenStatistics)
                        }
                        _ => unimplemented!(),
                    };

                    Ok((min_value, max_value))
                }
                _ => Err(StoreError::Internal("no stats".to_string())),
            }
        }
        _ => Err(StoreError::Internal(
            "compressed page should be data page".to_string(),
        )),
    }
}

pub type ColumnPath = Vec<String>;

#[derive(Debug)]
pub struct CompressedPageIterator<R> {
    reader: R,
    metadata: FileMetaData,
    row_group_cursors: HashMap<ColumnPath, usize>,
    chunk_buffer: HashMap<ColumnPath, VecDeque<CompressedPage>>,
    max_page_size: usize,
}

impl<R: Read + Seek> CompressedPageIterator<R> {
    pub fn try_new(mut reader: R, max_page_size: usize) -> Result<Self> {
        let metadata = parquet2::read::read_metadata(&mut reader)?;
        let chunk_buffer = metadata
            .schema()
            .columns()
            .iter()
            .map(|col| (col.path_in_schema.clone(), VecDeque::new()))
            .collect();
        let row_group_cursors = metadata
            .schema()
            .columns()
            .iter()
            .map(|col| (col.path_in_schema.clone(), 0))
            .collect();
        Ok(Self {
            reader,
            metadata,
            row_group_cursors,
            chunk_buffer,
            max_page_size,
        })
    }

    pub fn contains_column(&self, col_path: &ColumnPath) -> bool {
        let maybe_desc = self
            .metadata
            .schema()
            .columns()
            .iter()
            .find(|col| col.path_in_schema.eq(col_path));

        maybe_desc.is_some()
    }

    pub fn next_chunk(&mut self, col_path: &ColumnPath) -> Result<Option<Vec<CompressedPage>>> {
        if let Some(buf) = self.chunk_buffer.get_mut(col_path) {
            buf.clear()
        }

        let row_group_id = *self.row_group_cursors.get(col_path).unwrap();
        if row_group_id > self.metadata.row_groups.len() - 1 {
            return Ok(None);
        }

        let row_group = &self.metadata.row_groups[row_group_id];
        let column = row_group
            .columns()
            .iter()
            .find(|col| col.descriptor().path_in_schema.eq(col_path))
            .unwrap();

        let pages = parquet2::read::get_page_iterator(
            column,
            &mut self.reader,
            None,
            vec![],
            self.max_page_size,
        )?;

        let col = pages
            .into_iter()
            .map(|page| page.map_err(StoreError::from))
            .collect::<Result<Vec<_>>>()?;

        self.row_group_cursors
            .insert(col_path.to_owned(), row_group_id + 1);

        Ok(Some(col))
    }

    pub fn next_page(&mut self, col_path: &ColumnPath) -> Result<Option<CompressedPage>> {
        if !self.chunk_buffer.contains_key(col_path) {
            return Ok(None);
        }

        let need_next = self.chunk_buffer.get(col_path).unwrap().is_empty();
        let maybe_page = if need_next {
            match self.next_chunk(col_path)? {
                None => None,
                Some(pages) => match self.chunk_buffer.get_mut(col_path) {
                    Some(buf) => {
                        for page in pages {
                            buf.push_back(page);
                        }

                        buf.pop_front()
                    }
                    _ => unreachable!("uninitialized chunk buffer on column path: {col_path:?}"),
                },
            }
        } else {
            match self.chunk_buffer.get_mut(col_path) {
                Some(buf) => buf.pop_front(),
                _ => unreachable!("uninitialized chunk buffer on column path: {col_path:?}"),
            }
        };

        Ok(maybe_page)
    }
}

pub struct ArrowIteratorImpl {
    rdr: io::parquet::read::FileReader<BufReader<File>>,
}

impl ArrowIteratorImpl {
    pub fn new(mut rdr: BufReader<File>, fields: Vec<String>, chunk_size: usize) -> Result<Self> {
        // we can read its metadata:
        let metadata = io::parquet::read::read_metadata(&mut rdr)?;
        let schema = io::parquet::read::infer_schema(&metadata)?;
        let schema = schema.filter(|_, f| fields.contains(&f.name));
        let frdr = io::parquet::read::FileReader::new(
            rdr,
            metadata.row_groups,
            schema.clone(),
            Some(chunk_size),
            None,
            None,
        );
        Ok(Self { rdr: frdr })
    }
}

impl Iterator for ArrowIteratorImpl {
    type Item = Result<Chunk<Box<dyn Array>>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.rdr.next() {
            None => None,
            Some(Ok(chunk)) => Some(Ok(chunk)),
            Some(Err(err)) => Some(Err(err.into())),
        }
    }
}
