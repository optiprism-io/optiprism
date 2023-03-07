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

fn data_pages_to_arrays(pages: Vec<CompressedDataPage>, buf: &mut Vec<u8>) -> Result<Vec<Box<dyn Array>>> {
    pages
        .into_iter()
        .map(|page| data_page_to_array(page, buf))
        .collect::<Result<Vec<Box<dyn Array>>>>()
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

#[derive(Eq, PartialEq, PartialOrd, Ord, Debug, Clone, Copy)]
enum OptionalValue {
    Boolean(Option<bool>),
    Int32(Option<i32>),
    Int64(Option<i64>),
    Float(Option<f64>),
    Double(Option<f64>),
    ByteArray(Option<Vec<u8>>),
}

#[derive(Eq, PartialEq, PartialOrd, Ord, Debug, Clone, Copy)]
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

fn get_page_min_max_values(page: &CompressedDataPage) -> Result<(Value, Value)> {
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

    Ok((min_value, max_value))
}


type ReorderSlice = (usize, usize, usize);

#[derive(Debug)]
struct CompressedDataPagesRow {
    pages: Vec<CompressedDataPage>,
    min_values: Vec<Value>,
    max_values: Vec<Value>,
    stream: usize,
    reorder: Option<Vec<ReorderSlice>>,
}

impl CompressedDataPagesRow {
    pub fn new(pages: Vec<CompressedDataPage>, stream: usize) -> Self {
        let (min_values, max_values) = pages.iter().map(|page| {
            let (min, max) = get_page_min_max_values(&page).unwrap();
            (min, max)
        }).unzip();

        CompressedDataPagesRow {
            pages,
            min_values,
            max_values,
            stream,
            reorder: None,
        }
    }

    pub fn compress<T: ArrowRow>(row: T) -> Result<Self> {}
    pub fn min_values(&self) -> Vec<Value> {
        self.min_values.clone()
    }

    pub fn max_values(&self) -> Vec<Value> {
        self.max_values.clone()
    }
}


struct TwoColsArrowRow<A, B> {
    cols: Vec<Box<dyn Array>>,
    col1_type: PhantomData<A>,
    col2_type: PhantomData<B>,
}

trait ArrowRow {
    fn cols(&self) -> Vec<dyn Array>;
    fn min_values(&self) -> Vec<Value>;
    fn max_values(&self) -> Vec<Value>;
    fn decompress(row: CompressedDataPagesRow, buf: &mut Vec<u8>) -> Result<Self>;
}

impl<A, B> TwoColsArrowRow<A, B> {
    pub fn new(cols: Vec<Box<dyn Array>>) -> Self {
        Self {
            cols,
            col1_type: PhantomData,
            col2_type: PhantomData,
        }
    }

    pub fn downcast_arrs(&self) -> (&A, &B) {
        let col1 = self.cols[0].as_any().downcast_ref::<A>().unwrap();
        let col2 = self.cols[1].as_any().downcast_ref::<B>().unwrap();
        (col1, col2)
    }
}

impl ArrowRow for TwoColsArrowRow<PrimitiveArray<i64>, PrimitiveArray<i64>> {
    fn cols(&self) -> &[Box<dyn Array>] {
        self.cols.as_slice()
    }

    fn min_values(&self) -> Vec<Value> {
        let (col1, col2) = self.downcast_arrs();
        vec![Value::from(col1.value(0)), Value::from(col2.value(1))]
    }

    fn max_values(&self) -> Vec<Value> {
        let (col1, col2) = self.downcast_arrs();
        vec![
            Value::from(col1.value(self.col1.len() - 1)),
            Value::from(col2.value(self.col2.len() - 1)),
        ]
    }

    fn decompress(row: CompressedDataPagesRow, buf: &mut Vec<u8>) -> Result<Self> {
        let cols = data_pages_to_arrays(row.pages, buf)?;

        Ok(TwoColsArrowRow::new(cols)?)
    }
}


#[derive(Debug, Clone)]
struct TwoColRow<A, B>(usize, usize, A, B);

impl<A, B> Ord for TwoColRow<A, B> where A: Ord, B: Ord {
    fn cmp(&self, other: &Self) -> Ordering {
        (&self.2, &self.3).cmp(&(&other.2, &other.3))
    }
}

impl<A, B> PartialOrd for TwoColRow<A, B> where A: PartialOrd, B: PartialOrd {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<A, B> PartialEq for TwoColRow<A, B> where A: PartialEq, B: PartialEq {
    fn eq(&self, other: &Self) -> bool {
        (&self.0, &self.2, &self.3) == (&self.0, &other.2, &other.3)
    }
}

impl<A, B> Eq for TwoColRow<A, B> where A: Eq, B: Eq {}

impl Ord for TwoColRow {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.2, &self.3).cmp(&(other.2, &other.3))
    }
}

struct CompressedPagesRowIterator {}

impl Iterator for CompressedPagesRowIterator {
    type Item = Result<CompressedDataPagesRow>;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

struct CompressedPageIterator {}

impl Iterator for CompressedPageIterator {
    type Item = Result<CompressedDataPage>;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

struct FileMerger<'a, R, W, A>
    where
        R: Read,
        W: Write,
        A: ArrowRow
{
    readers: Vec<R>,
    index_pages_streams: Vec<CompressedPagesRowIterator>,
    data_pages_streams: Vec<CompressedPageIterator>,
    sorter: BinaryHeap<CompressedDataPagesRow>,
    merge_queue: Vec<CompressedDataPagesRow>,
    result: Vec<CompressedDataPagesRow>,
    writer: FileSeqWriter<W>,
    merge_threads: usize,
    pages_per_chunk: usize,
    sort_finished: bool,
    arrow_row_type: PhantomData<A>,
}

#[derive(Copy, Clone)]
struct Interval {
    min_values: Vec<Value>,
    max_values: Vec<Value>,
}


impl Interval {
    pub fn intersects(&self, other: Option<&CompressedDataPagesRow>) -> bool {
        if other.is_none() {
            false
        } else {
            let other = other.unwrap();
            self.min_values <= other.max_values() && self.max_values >= other.min_values
        }
    }
}

fn compressed_page_rows_interval(rows: &[CompressedDataPagesRow]) -> Interval {
    let mut min_values: Option<Vec<Value>> = None;
    let mut max_values: Option<Vec<Value>> = None;
    for row in rows.iter() {
        if min_values.is_none() {
            min_values = Some(row.min_values());
            max_values = Some(row.max_values());

            continue;
        }

        if row.min_values <= min_values.unwrap() {
            min_values = Some(row.min_values());
        }
        if row.max_values >= max_values.unwrap() {
            max_values = Some(row.max_values());
        }
    }

    Interval {
        min_values: min_values.unwrap(),
        max_values: max_values.unwrap(),
    }
}

impl<R, W, A> FileMerger<R, W, A>
    where
        R: Read,
        W: Write,
        A: ArrowRow,
{
    pub fn merge(mut index_pages_streams: Vec<CompressedPagesRowIterator>, data_pages_streams: Vec<CompressedPageIterator>, writer: W) -> Result<()> {
        let mut sorter = BinaryHeap::new();
        for stream in index_pages_streams.iter_mut() {
            if let Some(row) = stream.next() {
                sorter.push(*int);
            }
        }
        let mut m = Self {
            readers,
            index_pages_streams,
            data_pages_streams,
            sorter,
            merge_queue: vec![],
            result: vec![],
            writer,
            merge_threads: 10,
            pages_per_chunk: 2,
            sort_finished: false,
            arrow_row_type: PhantomData,
        };

        m.run()
    }

    fn merge_arrow_rows(&self, rows: Vec<A>) -> Result<Vec<A>> {}
    fn merge_compressed_page_rows(&self, rows: Vec<CompressedDataPagesRow>) -> Result<Vec<CompressedDataPagesRow>> {
        let mut buf = vec![];
        let arrs = rows.into_iter().map(|row| A::decompress(row, &mut buf)).collect::<Result<Vec<A>>>()?;
        let mr = self.merge_arrow_rows(arrs)?;

        let res = mr
            .into_iter()
            .map(|row| CompressedDataPagesRow::compress(row))
            .collect::<Result<Vec<CompressedDataPagesRow>>>()?;

        Ok(res)
    }


    fn merge_queue(&mut self) -> Result<()> {
        let mut merge_result = merge(self.merge_queue.drain(..).collect())?;
        println!("merge result {:?}", merge_result);
        self.result.append(&mut mr);

        Ok(())
    }

    fn next_index_chunk(&mut self) -> Result<Option<Vec<CompressedDataPagesRow>>> {
        while let Some(row) = self.sorter.pop() {
            println!("pop {:?}", row);
            if let Some(next) = self.index_pages_streamsstreams[row.stream].next() {
                println!("pop next {:?}", next);
                self.sorter.push(*next);
            }

            self.merge_queue.push(row);
            while compressed_page_rows_interval(&self.merge_queue).intersects(self.sorter.peek()) {
                println!("{:?} intersects with {:?}", row, self.sorter.peek());
                let next = self.sorter.pop().unwrap();
                if let Some(next) = self.index_pages_streams[next.stream].next() {
                    println!("pop next {:?}", next?);
                    sorter.push(next?);
                }
                self.merge_queue.push(next);
            }

            if self.merge_queue.len() > 1 {
                println!("merge queue {:?}", mq);
                self.merge_queue()?;
            } else {
                println!("push to result {:?}", row);
                self.result.push(self.merge_queue.pop().unwrap());
            }

            println!("result {:?}", self.result);
            if self.result.len() >= self.pages_per_chunk {
                let chunk = self.result.drain(..self.pages_per_chunk).collect();
                println!("return {:?}", chunk);
                return Ok(Some(chunk));
            }
        }

        if !self.result.is_empty() {
            let chunk = self.result.drain(..self.pages_per_chunk).collect();
            println!("return {:?}", chunk);
            return Ok(Some(chunk));
        }

        Ok(None)
    }

    fn run(&mut self) -> Result<()> {
        loop {
            let maybe_chunk = self.next_index_chunk()?;
            if maybe_chunk.is_none() {
                break;
            }
            let mut chunk = maybe_chunk.unwrap();
            let mut cols: Vec<Vec<CompressedPage>> = Vec::with_capacity(chunk[0].pages.len());

            for col in 0..chunk[0].pages.len() {
                for row in chunk.iter_mut() {
                    cols[col].push(CompressedPage::Data(row.pages.remove(col)));
                }
            }

            for col in cols.into_iter() {
                for page in col.into_iter() {
                    self.writer.write_page(&page)?;
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
