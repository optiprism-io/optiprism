use std::cmp::Ordering;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::io::Read;
use std::io::Seek;

use arrow2::array::Array;
use arrow2::datatypes::Field;
use arrow2::io::parquet::read::column_iter_to_arrays;
use arrow2::io::parquet::read::deserialize::page_iter_to_arrays;
use arrow2::io::parquet::read::schema::convert::to_primitive_type;
use arrow2::io::parquet::read::ParquetError;
use arrow2::io::parquet::write::array_to_columns;
use arrow2::io::parquet::write::WriteOptions;
use ordered_float::OrderedFloat;
use parquet2::compression::CompressionOptions;
use parquet2::encoding::Encoding;
use parquet2::metadata::ColumnDescriptor;
use parquet2::metadata::FileMetaData;
use parquet2::metadata::SchemaDescriptor;
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
use crate::parquet::merger::arrow::ArrowChunk;
use crate::parquet::merger::MergeReorder;

#[derive(Eq, PartialEq, PartialOrd, Ord, Debug, Clone)]
pub enum Value {
    Boolean(bool),
    Int32(i32),
    Int64(i64),
    Int96([u32; 3]),
    Float(OrderedFloat<f32>),
    Double(OrderedFloat<f64>),
    ByteArray(Vec<u8>),
}

impl From<bool> for Value {
    fn from(value: bool) -> Self {
        Value::Boolean(value)
    }
}

impl From<i32> for Value {
    fn from(value: i32) -> Self {
        Value::Int32(value)
    }
}

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Value::Int64(value)
    }
}

impl From<[u32; 3]> for Value {
    fn from(value: [u32; 3]) -> Self {
        Value::Int96(value)
    }
}

impl From<f32> for Value {
    fn from(value: f32) -> Self {
        Value::Float(OrderedFloat(value))
    }
}

impl From<f64> for Value {
    fn from(value: f64) -> Self {
        Value::Double(OrderedFloat(value))
    }
}

impl From<Vec<u8>> for Value {
    fn from(value: Vec<u8>) -> Self {
        Value::ByteArray(value)
    }
}

macro_rules! min_max_values {
    ($first_stats:expr,$last_stats:expr,$ty:ty) => {{
        match (
            $first_stats.as_any().downcast_ref::<$ty>(),
            $last_stats.as_any().downcast_ref::<$ty>(),
        ) {
            (Some(first), Some(last)) => (
                Value::from(first.min_value.clone().unwrap()),
                Value::from(last.max_value.clone().unwrap()),
            ),
            _ => return Err(StoreError::Internal("no stats".to_string())),
        }
    }};
}
pub fn check_intersection(chunks: &[PagesChunk], other: Option<&PagesChunk>) -> bool {
    if other.is_none() {
        return false;
    }

    let mut iter = chunks.iter();
    let first = iter.next().unwrap();
    let mut min_values = first.min_values();
    let mut max_values = first.max_values();
    for row in iter {
        if row.min_values <= min_values {
            min_values = row.min_values();
        }
        if row.max_values >= max_values {
            max_values = row.max_values();
        }
    }

    let other = other.unwrap();

    min_values <= other.max_values() && max_values >= other.min_values
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
        compression: CompressionOptions::Uncompressed, // todo
        version: Version::V2,
        data_pagesize_limit,
    };

    let pages = array_to_columns(arr, typ, opts, &[Encoding::Plain])?
        .pop()
        .unwrap();
    let compressed = pages
        .into_iter()
        .map(|page| compress(page.unwrap(), vec![], CompressionOptions::Uncompressed))
        .collect::<std::result::Result<Vec<CompressedPage>, _>>()?;

    Ok(compressed)
}

pub fn get_page_min_max_values(pages: &[CompressedPage]) -> Result<(Value, Value)> {
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

#[derive(Debug)]
pub struct MergedPagesChunk(pub PagesChunk, pub MergeReorder);

impl MergedPagesChunk {
    pub fn new(chunk: PagesChunk, merge_reorder: MergeReorder) -> Self {
        Self(chunk, merge_reorder)
    }

    pub fn num_values(&self) -> usize {
        self.0.num_values()
    }
}

#[derive(Debug)]
pub struct PagesChunk {
    pub cols: Vec<Vec<CompressedPage>>,
    pub min_values: Vec<Value>,
    pub max_values: Vec<Value>,
    pub stream: usize,
}

impl Eq for PagesChunk {}

impl PartialOrd for PagesChunk {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(other.cmp(self))
    }

    fn lt(&self, other: &Self) -> bool {
        other.min_values < self.min_values
    }
    #[inline]
    fn le(&self, other: &Self) -> bool {
        other.min_values <= self.min_values
    }
    #[inline]
    fn gt(&self, other: &Self) -> bool {
        other.min_values > self.min_values
    }
    #[inline]
    fn ge(&self, other: &Self) -> bool {
        other.min_values >= self.min_values
    }
}

impl Ord for PagesChunk {
    fn cmp(&self, other: &Self) -> Ordering {
        other.min_values.cmp(&self.min_values)
    }
}

impl PartialEq for PagesChunk {
    fn eq(&self, other: &Self) -> bool {
        self.min_values == other.min_values && self.max_values == other.max_values
    }
}

impl PagesChunk {
    pub fn new(cols: Vec<Vec<CompressedPage>>, stream: usize) -> Self {
        let (min_values, max_values) = cols
            .iter()
            .map(|pages| {
                let (min, max) = get_page_min_max_values(pages.as_slice()).unwrap();
                (min, max)
            })
            .unzip();

        Self {
            cols,
            min_values,
            max_values,
            stream,
        }
    }

    pub fn min_values(&self) -> Vec<Value> {
        self.min_values.clone()
    }

    pub fn max_values(&self) -> Vec<Value> {
        self.max_values.clone()
    }

    pub fn num_values(&self) -> usize {
        self.cols[0]
            .iter()
            .map(|page| match page {
                CompressedPage::Data(page) => page.num_values(),
                _ => unimplemented!("dictionary page is not supported"),
            })
            .sum()
    }
    pub fn to_arrow_chunk(
        self,
        buf: &mut Vec<u8>,
        index_cols: &[ColumnDescriptor],
    ) -> Result<ArrowChunk> {
        let cols = self
            .cols
            .into_iter()
            .zip(index_cols.iter())
            .map(|(pages, cd)| {
                pages_to_arrays(pages, cd, None, buf).map(|mut res| res.pop().unwrap())
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(ArrowChunk::new(cols, self.stream))
    }

    pub fn from_arrow(arrs: &[Box<dyn Array>], index_cols: &[ColumnDescriptor]) -> Result<Self> {
        let opts = WriteOptions {
            write_statistics: true,
            compression: CompressionOptions::Snappy, // todo
            version: Version::V2,
            data_pagesize_limit: None,
        };
        let cols = arrs
            .iter()
            .zip(index_cols.iter())
            .map(|(arr, cd)| {
                array_to_columns(arr, cd.base_type.clone(), opts, &[Encoding::Plain])
                    .and_then(|mut res| {
                        res.pop()
                            .unwrap()
                            .collect::<arrow2::error::Result<Vec<_>>>()
                    })
                    .and_then(|pages| {
                        pages
                            .into_iter()
                            .map(|page| compress(page, vec![], CompressionOptions::Snappy))
                            .collect::<parquet2::error::Result<Vec<_>>>()
                            .map_err(|e| arrow2::error::Error::from_external_error(Box::new(e)))
                    })
            })
            .collect::<arrow2::error::Result<Vec<_>>>()?;

        Ok(Self::new(cols, 0))
    }
}

pub type ColumnPath = Vec<String>;

pub struct CompressedPageIterator<R> {
    reader: R,
    metadata: FileMetaData,
    row_group_cursors: HashMap<ColumnPath, usize>,
    chunk_buffer: HashMap<ColumnPath, VecDeque<CompressedPage>>,
    max_page_size: usize,
}

impl<R: Read + Seek> CompressedPageIterator<R> {
    pub fn try_new(mut reader: R) -> Result<Self> {
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
            max_page_size: 1024 * 1024,
        })
    }

    pub fn _schema(&self) -> &SchemaDescriptor {
        self.metadata.schema()
    }

    pub fn _get_col_path(&self, col_id: usize) -> ColumnPath {
        self.metadata.schema().columns()[col_id]
            .path_in_schema
            .clone()
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
