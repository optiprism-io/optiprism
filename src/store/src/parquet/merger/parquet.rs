use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::io::Read;
use std::io::Seek;
use std::sync::Arc;
use std::sync::Mutex;

use arrow2::array::new_null_array;
use arrow2::array::Array;
use arrow2::array::Int32Array;
use arrow2::array::Int64Array;
use arrow2::chunk::Chunk;
use arrow2::datatypes::DataType;
use arrow2::datatypes::Field;
use arrow2::datatypes::Schema;
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
use parquet2::read::PageReader;
use parquet2::schema::types::ParquetType;
use parquet2::schema::types::PhysicalType;
use parquet2::statistics::BinaryStatistics;
use parquet2::statistics::FixedLenStatistics;
use parquet2::statistics::PrimitiveStatistics;
use parquet2::write::compress;
use parquet2::write::Version;

use crate::error::Result;
use crate::error::StoreError;
use crate::parquet::merger::IndexChunk;

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
        println!("{:?}", metadata.schema().columns());
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
        let mut need_next = false;
        match self.chunk_buffer.get(col_path) {
            None => return Ok(None),
            Some(p) => need_next = p.is_empty(),
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

pub struct ArrowIterator<R: Read + Seek> {
    page_iter: CompressedPageIterator<R>,
    fields: Vec<ColumnDescriptor>,
    schema: Schema,
}

impl<R: Read + Seek> ArrowIterator<R> {
    pub fn new(
        page_iter: CompressedPageIterator<R>,
        fields: Vec<ColumnDescriptor>,
        schema: Schema,
    ) -> Self {
        Self {
            page_iter,
            fields,
            schema,
        }
    }

    pub fn contains_column(&self, col_path: &ColumnPath) -> bool {
        self.page_iter.contains_column(col_path)
    }

    pub fn next(&mut self) -> Result<Option<Chunk<Box<dyn Array>>>> {
        let mut ret = vec![];

        let mut num_rows = 0;
        for (idx, cd) in self.fields.iter().enumerate() {
            let page = self.page_iter.next_page(&cd.path_in_schema)?;
            match page {
                None => {
                    // no more pages
                    if idx == 0 {
                        return Ok(None);
                    }

                    ret.push(new_null_array(
                        self.schema.fields[idx].data_type().clone(),
                        num_rows,
                    ))
                }
                Some(page) => {
                    let mut buf = vec![];
                    let mut arrs = pages_to_arrays(vec![page], cd, None, &mut buf)?;
                    let arr = arrs.pop().unwrap();
                    // set rows. Assume that first column is always present
                    if idx == 0 {
                        num_rows = arr.len();
                    }
                    ret.push(arr);
                }
            }
        }

        Ok(Some(Chunk::new(ret)))
    }
}
