use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::io::Read;
use std::io::Write;

use arrow2::array::Array;
use arrow2::array::PrimitiveArray;
use parquet2::metadata::ColumnDescriptor;
use parquet2::metadata::SchemaDescriptor;
use parquet2::page::{CompressedDataPage, CompressedPage};
use parquet2::read::PageReader;
use parquet2::write::{Compressor, FileSeqWriter, Version};
use rayon::prelude::*;
use arrow2::datatypes::{DataType, PrimitiveType, TimeUnit};
use arrow2::io::parquet::read::deserialize::page_iter_to_arrays;
use arrow2::io::parquet::read::deserialize::simple::data_page_to_array;
use parquet2::schema::types::{ParquetType, PhysicalType};
use parquet2::statistics::{BinaryStatistics, BooleanStatistics, FixedLenStatistics, PrimitiveStatistics};
use store::error::{Result, StoreError};
use arrow2::io::parquet::read::decompress;
use arrow2::io::parquet::write::{array_to_columns, array_to_page_simple, WriteOptions};
use parquet2::compression::CompressionOptions;
use parquet2::encoding::Encoding;

fn from_physical_type(t: &PhysicalType) -> DataType {
    match t {
        PhysicalType::Boolean => DataType::Boolean,
        PhysicalType::Int32 => DataType::Int32,
        PhysicalType::Int64 => DataType::Int64,
        PhysicalType::Float => DataType::Float32,
        PhysicalType::Double => DataType::Float64,
        PhysicalType::ByteArray => DataType::Utf8,
        PhysicalType::FixedLenByteArray(l) => DataType::FixedSizeBinary(*l),
        PhysicalType::Int96 => DataType::Timestamp(TimeUnit::Nanosecond, None),
    }
}

fn data_page_to_array(page: CompressedDataPage, buf: &mut Vec<u8>) -> Result<Box<dyn Array>> {
    let stats = page.statistics().unwrap()?;

    let num_rows = page.num_values() + stats.null_count().or_else(|| Some(0)).unwrap() as usize;
    let physical_type = stats.physical_type();
    let primitive_type = PrimitiveType::from_physical("f".to_string(), physical_type.to_owned());
    let data_type = from_physical_type(physical_type);
    let decompressed_page = decompress(CompressedPage::Data(page), buf)?;
    let iter = fallible_streaming_iterator::convert(std::iter::once(Ok(&decompressed_page)));
    let mut r = page_iter_to_arrays(iter, &primitive_type, data_type, None, num_rows)?;

    Ok(r.next().unwrap()?)
}

#[derive(Debug)]
struct ReorderSlices {}

// type MergeTask = Vec<SortItem>;

// struct MergeResult(Vec<SortItem>);

// type MergeItem = (Vec<ArrayWithStats>, Option<usize>);


/*enum SortItem {
    Merged(Vec<ArrayWithStats>),
    Unmerged(Vec<CompressedDataPageWithStats>, usize),
}
*/
fn cmp_values(lhs: &[Value], rhs: &[Value]) -> std::cmp::Ordering {
    for (l, r) in lhs.iter().zip(rhs.iter()) {
        match (*l).cmp(r) {
            std::cmp::Ordering::Equal => continue,
            x => return x
        }
    }
    std::cmp::Ordering::Equal
}

impl SortItem {
    pub fn new_merged(arrs: Vec<ArrayWithStats>) -> Self {
        Self::Merged(arrs)
    }

    pub fn new_unmerged(pages: Vec<CompressedDataPageWithStats>, stream: usize) -> Self {
        Self::Unmerged(pages, stream)
    }

    pub fn min_values(&self) -> Vec<Value> {
        match self {
            Self::Merged(arrs) => arrs.iter().map(|arr| arr.min_value).collect(),
            Self::Unmerged(pages, _) => pages.iter().map(|page| page.min_value).collect(),
        }
    }

    pub fn max_values(&self) -> Vec<Value> {
        match self {
            Self::Merged(arrs) => arrs.iter().map(|arr| arr.max_value).collect(),
            Self::Unmerged(pages, _) => pages.iter().map(|page| page.max_value).collect(),
        }
    }
}

/*impl From<MergeResult> for SortItem {
    fn from(result: MergeResult) -> Self {
        Self::new_merged(result.0.into_iter().next().unwrap())
    }
}*/

#[defer(Eq, PartialEq, PartialOrd, Ord, Debug, Clone, Copy)]
enum OptionalValue {
    Boolean(Option<bool>),
    Int32(Option<i32>),
    Int64(Option<i64>),
    Float(Option<f64>),
    Double(Option<f64>),
    ByteArray(Option<Vec<u8>>),
}

#[defer(Eq, PartialEq, PartialOrd, Ord, Debug, Clone, Copy)]
enum Value {
    Boolean(bool),
    Int32(i32),
    Int64(i64),
    Int96([u32; 3]),
    Float(f32),
    Double(f64),
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
        Value::Float(value)
    }
}

impl From<f64> for Value {
    fn from(value: f64) -> Self {
        Value::Double(value)
    }
}

impl From<Vec<u8>> for Value {
    fn from(value: Vec<u8>) -> Self {
        Value::ByteArray(value)
    }
}

#[derive(Debug)]
struct PagesRow {
    pages: Vec<Page>,
    stream: Option<usize>,
}

impl PagesRow {
    pub fn from_compressed(pages: Vec<CompressedDataPage>, stream: usize) -> Self {
        Self {
            pages: pages.into_iter().map(|page| Page::from_compressed(page, None).unwrap()).collect(),
            stream: Some(stream),
        }
    }

    pub fn min_values(&self) -> Vec<Value> {
        self.pages.iter().map(|page| page.min_value).collect()
    }
    pub fn max_values(&self) -> Vec<Value> {
        self.pages.iter().map(|page| page.max_value).collect()
    }
}

#[derive(Debug)]
enum PageData {
    Compressed(CompressedDataPage),
    Array(Box<dyn Array>),
}

#[derive(Debug)]
struct Page {
    data: PageData,
    min_value: Value,
    max_value: Value,
    reorder: Option<ReorderSlices>,
}

impl Page {
    pub fn from_compressed(data: CompressedDataPage, reorder: Option<ReorderSlices>) -> Result<Self> {
        let stats = page.statistics();
        if stats.is_none() {
            return Err(StoreError::Internal("no stats".to_string()));
        }

        let stats = stats.unwrap()?;

        let (min_value, max_value) = match stats.physical_type() {
            PhysicalType::Boolean => {
                let stats = stats.as_any().downcast_ref::<BooleanStatistics>().unwrap();
                if stats.min_value.is_none() || stats.max_value.is_none() {
                    return Err(StoreError::Internal("no stats".to_string()));
                }
                (Value::from(stats.min_value.unwrap()), Value::from(stats.max_value.unwrap()))
            }
            PhysicalType::Int32 => {
                let stats = stats.as_any().downcast_ref::<PrimitiveStatistics<i32>>().unwrap();
                if stats.min_value.is_none() || stats.max_value.is_none() {
                    return Err(StoreError::Internal("no stats".to_string()));
                }
                (Value::from(stats.min_value.unwrap()), Value::from(stats.max_value.unwrap()))
            }
            PhysicalType::Int64 => {
                let stats = stats.as_any().downcast_ref::<PrimitiveStatistics<i64>>().unwrap();
                if stats.min_value.is_none() || stats.max_value.is_none() {
                    return Err(StoreError::Internal("no stats".to_string()));
                }
                (Value::from(stats.min_value.unwrap()), Value::from(stats.max_value.unwrap()))
            }
            PhysicalType::Int96 => {
                let stats = stats.as_any().downcast_ref::<PrimitiveStatistics<[u32; 3]>>().unwrap();
                if stats.min_value.is_none() || stats.max_value.is_none() {
                    return Err(StoreError::Internal("no stats".to_string()));
                }
                (Value::from(stats.min_value.unwrap()), Value::from(stats.max_value.unwrap()))
            }
            PhysicalType::Float => {
                let stats = stats.as_any().downcast_ref::<PrimitiveStatistics<f32>>().unwrap();
                if stats.min_value.is_none() || stats.max_value.is_none() {
                    return Err(StoreError::Internal("no stats".to_string()));
                }
                (Value::from(stats.min_value.unwrap()), Value::from(stats.max_value.unwrap()))
            }
            PhysicalType::Double => {
                let stats = stats.as_any().downcast_ref::<PrimitiveStatistics<f64>>().unwrap();
                if stats.min_value.is_none() || stats.max_value.is_none() {
                    return Err(StoreError::Internal("no stats".to_string()));
                }
                (Value::from(stats.min_value.unwrap()), Value::from(stats.max_value.unwrap()))
            }
            PhysicalType::ByteArray => {
                let stats = stats.as_any().downcast_ref::<BinaryStatistics>().unwrap();
                if stats.min_value.is_none() || stats.max_value.is_none() {
                    return Err(StoreError::Internal("no stats".to_string()));
                }
                (Value::from(stats.min_value.unwrap()), Value::from(stats.max_value.unwrap()))
            }
            PhysicalType::FixedLenByteArray(_) => {
                let stats = stats.as_any().downcast_ref::<FixedLenStatistics>().unwrap();
                if stats.min_value.is_none() || stats.max_value.is_none() {
                    return Err(StoreError::Internal("no stats".to_string()));
                }
                (Value::from(stats.min_value.unwrap()), Value::from(stats.max_value.unwrap()))
            }
        };

        Ok(Self {
            data: PageData::Compressed(data),
            min_value,
            max_value,
            reorder,
        })
    }

    pub fn from_array(array: Box<dyn Array>, min_value: Value, max_value: Value, reorder: Option<ReorderSlices>) -> Self {
        Self {
            data: PageData::Array(array),
            min_value,
            max_value,
            reorder,
        }
    }

    pub fn to_compressed(self) -> CompressedPage {
        match self.data {
            PageData::Compressed(page) => CompressedPage::Data(page),
            PageData::Array(arr) => panic!("not compressed"),
        }
    }
}

#[derive(Debug, Clone)]
struct Int64Int64Row(usize, usize, i64, i64);

impl Ord for Int64Int64Row {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.2, &self.3).cmp(&(other.2, &other.3))
    }
}

impl PartialOrd for Int64Int64Row {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Int64Int64Row {
    fn eq(&self, other: &Self) -> bool {
        (self.0, self.2, &self.3) == (self.0, other.2, &other.3)
    }
}

impl Eq for Int64Int64Row {}

struct CompressedPagesRowIterator {}

impl Iterator for CompressedPagesRowIterator {
    type Item = Result<Vec<CompressedDataPage>>;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

struct FileMerger<'a, R, W>
    where
        R: Read,
        W: Write,
{
    readers: Vec<R>,
    index_pages_stream: Vec<CompressedPagesRowIterator>,
    schemas: Vec<SchemaDescriptor>,
    schema: SchemaDescriptor,
    data_columns: Vec<ColumnDescriptor>,
    sorter: BinaryHeap<PagesRow>,
    sort_finished: bool,
    merge_queue: Vec<Vec<PagesRow>>,
    merge_result: BinaryHeap<PagesRow>,
    writer: FileSeqWriter<W>,
    merge_threads: usize,
    pages_per_chunk: usize,
}

fn merge_int64_int64_arrays(streams: Vec<(PrimitiveArray<i64>, PrimitiveArray<i64>)>) -> Vec<Int64Int64Row> {
    let mut vals: Vec<Int64Int64Row> = streams
        .into_iter()
        .map(|(stream, c1, c2)| c1.values_iter().zip(c2.values_iter()).enumerate()
            .map(|(row_id, (v1, v2))| Int64Int64Row(stream, row_id, *v1, *v2))
            .collect::<Vec<Int64Int64Row>>()).flatten().collect();

    vals.sort();

    vals
}

fn merge(rows: Vec<PagesRow>) -> Result<Vec<PagesRow>> {}

impl<R, W> FileMerger<R, W>
    where
        R: Read,
        W: Write,
{
    pub fn merge(readers: Vec<R>, schemas: Vec<SchemaDescriptor>, writer: W) -> Result<()> {
        let mut m = Self {
            readers,
            index_pages_stream: vec![],
            schemas,
            schema: (),
            data_columns: vec![],
            sorter: (),
            sort_finished: false,
            merge_queue: vec![],
            merge_result: (),
            writer,
            merge_threads: 0,
            pages_per_chunk: 0,
        };

        m._merge()
    }

    fn next_index_rows(&mut self) -> Result<Option<Vec<PagesRow>>> {
        if self.sort_finished {
            return Ok(None);
        }

        loop {
            // loop over all streams and push the next page into the sorter if it is not already there
            for (stream_idx, stream) in self.index_pages_stream.iter_mut().enumerate() {
                let mut in_sorter = false;
                for item in self.sorter.iter() {
                    // unmerged
                    if let Some(sid) = item.stream {
                        if sid == stream_idx {
                            in_sorter = true;
                            break;
                        }
                    }
                }

                // if not in sorter, push the next page from the stream
                if !in_sorter {
                    // take the next page from the stream
                    match stream.next() {
                        Some(pages) => {
                            let item = PagesRow::from_compressed(pages?, stream_idx);
                            // sorter will sort the page rows
                            self.sorter.push(item);
                        }
                        // stream is finished, just ignore
                        _ => {}
                    }
                }
            }

            if let Some(mr) = self.merge_result.pop() {
                self.sorter.push(mr);
            }

            if self.sorter.is_empty() {
                if self.merge_queue.is_empty() {
                    self.sort_finished = true;
                    return Ok(None);
                }

                let res: Vec<PagesRow> = self.merge_queue
                    .par_drain(..)
                    .map(|v| merge(v))
                    .collect::<Result<_>>()?;

                res.into_iter().for_each(|v| self.merge_result.push(v));

                self.sorter.push(self.merge_result.pop().unwrap());
            }

            let mut out = Vec::new();
            let first = self.sorter.pop().unwrap();
            let (mut min, mut max) = (first.min_values(), first.max_values());
            out.push(first);

            while !self.sorter.is_empty() {
                match self.sorter.peek() {
                    None => break,
                    Some(_) => {
                        let pr = self.sorter.pop().unwrap();
                        let (pr_min, pr_max) = (pr.min_values(), pr.max_values());
                        if pr_min >= min && pr_min <= max {
                            out.push(pr);
                            if pr_max > max {
                                max = pr_max;
                            }
                        } else {
                            break;
                        }
                    }
                }
            }

            if out.len() == 1 {
                return Ok(Some(out));
            }
            println!("merge {:?} to ({min},{max})", out);
            self.merge_queue.push(out);
            // it is time to merge
            if self.merge_queue.len() == self.merge_threads {
                println!("merge of queue: {:?}", self.merge_queue);
                let res: Vec<PagesRow> = self.merge_queue
                    .par_drain(..)
                    .map(|v| merge(v))
                    .collect::<Result<_>>()?;

                res.into_iter().for_each(|v| self.merge_result.push(v));
            }
        }
    }
    fn next_index_chunk(
        &mut self,
    ) -> Result<Option<Vec<PagesRow>>> {
        if self.sort_finished {
            return Ok(None);
        }

        let mut result: Vec<PagesRow> = Vec::with_capacity(self.pages_per_chunk);
        for _ in 0..self.pages_per_chunk {
            if result.len() >= self.pages_per_chunk {
                break;
            }
            match self.next_index_rows()? {
                None => {
                    break;
                }
                Some(mut rows) => {
                    result.append(&mut rows);
                }
            }
        }

        if result.is_empty() {
            return Ok(None);
        }

        Ok(Some(result))
    }
    fn next_data_chunk(
        &mut self,
        col: &ColumnDescriptor,
        slices: &[ReorderSlices],
    ) -> Result<Vec<CompressedPage>> {
        todo!()
    }

    fn _merge(&mut self) -> Result<()> {
        loop {
            let maybe_chunk = self.next_index_chunk()?;
            if maybe_chunk.is_none() {
                break;
            }
            let rows: Vec<Vec<Page>> = maybe_chunk.unwrap().into_iter().map(|pr| pr.pages).collect();
            let len = rows[0].len();
            let mut iters: Vec<_> = rows.into_iter().map(|n| n.into_iter()).collect();
            let cols: Vec<Vec<Page>> = (0..len)
                .map(|_| {
                    iters
                        .iter_mut()
                        .map(|n| n.next().unwrap())
                        .collect::<Vec<Page>>()
                })
                .collect();

            for (col_id, pages) in cols.into_iter().enumerate() {
                for page in pages.into_iter() {
                    match page.data {
                        PageData::Compressed(data_page) => {
                            let page = CompressedPage::Data(data_page);
                            self.writer.write_page(&page)?;
                        }
                        PageData::Array(arr) => {
                            let opts = WriteOptions {
                                write_statistics: true,
                                compression: CompressionOptions::Snappy, // todo
                                version: Version::V1,
                                data_pagesize_limit: None,
                            };
                            let ParquetType::PrimitiveType(typ) = self.schema.fields()[col_id].clone();
                            let page = array_to_page_simple(&arr, typ, opts, Encoding::Plain)?;

                            let compressor = Compressor::new(vec![page], opts.compression, vec![]);
                            let compressed_pages = compressor.collect::<Result<Vec<_>>>()?;
                            for page in compressed_pages {
                                self.writer.write_page(&page)?;
                            };
                        }
                    }
                }

                self.writer.end_column()?;
            }

            /*for col in self.data_columns.iter() {
                for page in self.next_data_chunk(col, &slices)? {
                    self.writer.write_page(&page)?;
                }
                self.writer.end_column()?;
            }*/

            self.writer.end_row_group()?;
        }

        self.writer.end(None)?;

        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    todo!()
}
