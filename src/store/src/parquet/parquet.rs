use std::cmp::Ordering;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::io::Read;
use std::io::Seek;

use arrow2::array::Array;
use arrow2::io::parquet::read::deserialize::page_iter_to_arrays;
use arrow2::io::parquet::write::array_to_page_simple;
use arrow2::io::parquet::write::WriteOptions;
use futures::stream::iter;
use futures::StreamExt;
use parquet2::compression::CompressionOptions;
use parquet2::encoding::Encoding;
use parquet2::metadata::FileMetaData;
use parquet2::metadata::RowGroupMetaData;
use parquet2::metadata::SchemaDescriptor;
use parquet2::page::CompressedDataPage;
use parquet2::page::CompressedPage;
use parquet2::page::Page;
use parquet2::read::decompress;
use parquet2::schema::types::ParquetType;
use parquet2::schema::types::PhysicalType;
use parquet2::schema::types::PrimitiveType;
use parquet2::statistics::BinaryStatistics;
use parquet2::statistics::BooleanStatistics;
use parquet2::statistics::FixedLenStatistics;
use parquet2::statistics::PrimitiveStatistics;
use parquet2::write::compress;
use parquet2::write::Compressor;
use parquet2::write::Version;

use crate::error::Result;
use crate::error::StoreError;
use crate::parquet::arrow::ArrowRow;
use crate::parquet::from_physical_type;
use crate::parquet::ReorderSlice;
use crate::parquet::Value;

pub fn check_intersection(
    rows: &[CompressedDataPagesRow],
    other: Option<&CompressedDataPagesRow>,
) -> bool {
    if other.is_none() {
        return false;
    }

    let mut iter = rows.iter();
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

pub fn data_pages_to_arrays(
    pages: Vec<CompressedDataPage>,
    buf: &mut Vec<u8>,
) -> Result<Vec<Box<dyn Array>>> {
    pages
        .into_iter()
        .map(|page| data_page_to_array(page, buf))
        .collect::<Result<Vec<Box<dyn Array>>>>()
}

pub fn data_page_to_array(page: CompressedDataPage, buf: &mut Vec<u8>) -> Result<Box<dyn Array>> {
    let stats = page.statistics().unwrap()?;

    // let num_rows = page.num_values() + stats.null_count().or_else(|| Some(0)).unwrap() as usize;
    let num_rows = page.num_values();
    let physical_type = stats.physical_type();
    let primitive_type = PrimitiveType::from_physical("f".to_string(), physical_type.to_owned());
    let data_type = from_physical_type(physical_type);
    let decompressed_page = decompress(CompressedPage::Data(page), buf)?;
    let iter = fallible_streaming_iterator::convert(std::iter::once(Ok(&decompressed_page)));
    let mut r = page_iter_to_arrays(iter, &primitive_type, data_type, None, num_rows)?;
    Ok(r.next().unwrap()?)
}

pub fn array_to_page(arr: Box<dyn Array>, typ: PrimitiveType, buf: Vec<u8>) -> Result<CompressedPage> {
    let mut arrs = arrays_to_pages(
        &vec![arr],
        vec![typ],
        buf,
    )?;

    Ok(CompressedPage::Data(arrs.pop().unwrap()))
}

pub fn arrays_to_pages(
    arrs: &[Box<dyn Array>],
    types: Vec<PrimitiveType>,
    buf: Vec<u8>,
) -> Result<Vec<CompressedDataPage>> {
    let opts = WriteOptions {
        write_statistics: true,
        compression: CompressionOptions::Snappy, // todo
        version: Version::V2,
        data_pagesize_limit: None,
    };

    let pages = arrs
        .into_iter()
        .zip(types)
        .map(|(arr, typ)| array_to_page_simple(arr.as_ref(), typ, opts, Encoding::Plain))
        .collect::<std::result::Result<Vec<Page>, _>>()?;

    let cp = pages
        .into_iter()
        .map(|page| compress(page, vec![], CompressionOptions::Snappy))
        .collect::<std::result::Result<Vec<CompressedPage>, _>>()?;

    let r = cp
        .into_iter()
        .map(|p| {
            if let CompressedPage::Data(dp) = p {
                dp
            } else {
                unimplemented!("dicts are not supported")
            }
        })
        .collect::<Vec<_>>();

    Ok(r)
}

pub fn get_page_min_max_values(page: &CompressedDataPage) -> Result<(Value, Value)> {
    let stats = page.statistics();
    if stats.is_none() {
        return Err(StoreError::Internal("no stats".to_string()));
    }

    let stats = stats.unwrap()?;

    let (min_value, max_value) = match stats.physical_type() {
        PhysicalType::Int32 => {
            let stats = stats
                .as_any()
                .downcast_ref::<PrimitiveStatistics<i32>>()
                .unwrap();
            if stats.min_value.is_none() || stats.max_value.is_none() {
                return Err(StoreError::Internal("no int32 stats".to_string()));
            }
            (
                Value::from(stats.min_value.unwrap()),
                Value::from(stats.max_value.unwrap()),
            )
        }
        PhysicalType::Int64 => {
            let stats = stats
                .as_any()
                .downcast_ref::<PrimitiveStatistics<i64>>()
                .unwrap();
            if stats.min_value.is_none() || stats.max_value.is_none() {
                return Err(StoreError::Internal("no int64 stats".to_string()));
            }
            (
                Value::from(stats.min_value.unwrap()),
                Value::from(stats.max_value.unwrap()),
            )
        }
        PhysicalType::Int96 => {
            let stats = stats
                .as_any()
                .downcast_ref::<PrimitiveStatistics<[u32; 3]>>()
                .unwrap();
            if stats.min_value.is_none() || stats.max_value.is_none() {
                return Err(StoreError::Internal("no int96 stats".to_string()));
            }
            (
                Value::from(stats.min_value.unwrap()),
                Value::from(stats.max_value.unwrap()),
            )
        }
        PhysicalType::Float => {
            let stats = stats
                .as_any()
                .downcast_ref::<PrimitiveStatistics<f32>>()
                .unwrap();
            if stats.min_value.is_none() || stats.max_value.is_none() {
                return Err(StoreError::Internal("no float32 stats".to_string()));
            }
            (
                Value::from(stats.min_value.unwrap()),
                Value::from(stats.max_value.unwrap()),
            )
        }
        PhysicalType::Double => {
            let stats = stats
                .as_any()
                .downcast_ref::<PrimitiveStatistics<f64>>()
                .unwrap();
            if stats.min_value.is_none() || stats.max_value.is_none() {
                return Err(StoreError::Internal("no float64 stats".to_string()));
            }
            (
                Value::from(stats.min_value.unwrap()),
                Value::from(stats.max_value.unwrap()),
            )
        }
        PhysicalType::ByteArray => {
            let stats = stats
                .as_any()
                .downcast_ref::<BinaryStatistics>()
                .unwrap()
                .to_owned();
            // println!("ss1 {:#?}", stats);
            if stats.min_value.is_none() || stats.max_value.is_none() {
                return Err(StoreError::Internal("no byte array stats".to_string()));
            }
            (
                Value::from(stats.min_value.unwrap()),
                Value::from(stats.max_value.unwrap()),
            )
        }
        PhysicalType::FixedLenByteArray(_) => {
            let stats = stats
                .as_any()
                .downcast_ref::<FixedLenStatistics>()
                .unwrap()
                .to_owned();
            if stats.min_value.is_none() || stats.max_value.is_none() {
                return Err(StoreError::Internal(
                    "no fixed len byte array stats".to_string(),
                ));
            }
            (
                Value::from(stats.min_value.unwrap()),
                Value::from(stats.max_value.unwrap()),
            )
        }
        _ => unimplemented!(),
    };

    Ok((min_value, max_value))
}

pub type CompressedDataPagesColumns = Vec<Vec<CompressedPage>>;

#[derive(Debug)]
pub struct CompressedDataPagesRow {
    pub pages: Vec<CompressedDataPage>,
    pub min_values: Vec<Value>,
    pub max_values: Vec<Value>,
    pub stream: usize,
}

impl Eq for CompressedDataPagesRow {}

impl PartialOrd for CompressedDataPagesRow {
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

impl Ord for CompressedDataPagesRow {
    fn cmp(&self, other: &Self) -> Ordering {
        other.min_values.cmp(&self.min_values)
    }
}

impl PartialEq for CompressedDataPagesRow {
    fn eq(&self, other: &Self) -> bool {
        self.min_values == other.min_values && self.max_values == other.max_values
    }
}

impl CompressedDataPagesRow {
    pub fn new_from_pages(pages: Vec<CompressedPage>, stream: usize) -> Self {
        let data_pages = pages
            .into_iter()
            .map(|p| {
                if let CompressedPage::Data(dp) = p {
                    dp
                } else {
                    unimplemented!("dicts are not supported")
                }
            })
            .collect();
        Self::new(data_pages, stream)
    }

    pub fn new(pages: Vec<CompressedDataPage>, stream: usize) -> Self {
        let (min_values, max_values) = pages
            .iter()
            .map(|page| {
                let (min, max) = get_page_min_max_values(&page).unwrap();
                (min, max)
            })
            .unzip();

        CompressedDataPagesRow {
            pages,
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

    pub fn to_arrow_row(self, buf: &mut Vec<u8>) -> Result<ArrowRow> {
        let arrs = data_pages_to_arrays(self.pages, buf)?;

        Ok(ArrowRow::new(arrs, self.stream))
    }

    pub fn from_arrow_row(row: ArrowRow, types: Vec<PrimitiveType>, buf: Vec<u8>) -> Result<Self> {
        let res = arrays_to_pages(&row.arrs, types, buf)?;
        Ok(CompressedDataPagesRow::new(res, row.stream))
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
        Ok(Self {
            reader,
            metadata,
            row_group_cursors: HashMap::new(),
            chunk_buffer: HashMap::new(),
            max_page_size: 1024 * 1024,
        })
    }

    pub fn schema(&self) -> &SchemaDescriptor {
        self.metadata.schema()
    }

    pub fn get_col_path(&self, col_id: usize) -> ColumnPath {
        self.metadata.schema().columns()[col_id]
            .path_in_schema
            .clone()
    }

    pub fn contains_column(&self, col_path: &ColumnPath) -> bool {
        self.metadata
            .schema()
            .columns()
            .iter()
            .find(|col| col.path_in_schema.eq(col_path))
            .is_some()
    }

    pub fn next_page(&mut self, col_path: &ColumnPath) -> Result<Option<CompressedPage>> {
        if !self.contains_column(col_path) {
            return Err(StoreError::Internal(format!(
                "column {:?} not found",
                col_path
            )));
        }

        if let Some(buf) = self.chunk_buffer.get_mut(col_path) {
            if let Some(page) = buf.pop_front() {
                println!(">");

                return Ok(Some(page));
            }
        }

        let row_group_id = *(self.row_group_cursors.get(col_path).or(Some(&0)).unwrap());
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

        for page in pages {
            let page = page?;
            match self.chunk_buffer.get_mut(col_path) {
                Some(buf) => buf.push_back(page),
                None => {
                    self.chunk_buffer
                        .insert(col_path.to_owned(), VecDeque::from(vec![page]));
                }
            }
        }
        self.row_group_cursors
            .insert(col_path.to_owned(), row_group_id + 1);

        println!("$$");
        self.next_page(col_path)
    }
}
