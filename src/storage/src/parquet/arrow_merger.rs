use std::cmp::min;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::collections::VecDeque;
use std::fs::File;
use std::io::BufReader;
use arrow2::array::growable::make_growable;
use arrow2::array::new_null_array;
use arrow2::array::Array;
use arrow2::array::PrimitiveArray;
use arrow2::chunk::Chunk;
use arrow2::datatypes::DataType;
use arrow2::datatypes::Schema;
use arrow2::io::parquet::read::infer_schema;
use arrow2::io::parquet::write::to_parquet_schema;
use arrow2::types::NativeType;
use metrics::counter;
use parquet2::metadata::ColumnDescriptor;

use crate::error::Result;
use crate::error::StoreError;
use crate::parquet::chunk_min_max;
use crate::parquet::try_merge_arrow_schemas;
use crate::parquet::ArrowIteratorImpl;
use crate::parquet::OneColMergeRow;
use crate::parquet::TwoColMergeRow;
use crate::KeyValue;

// merge chunks with single column partition which is primitive

pub fn merge_one_primitive<T: NativeType + Ord>(
    chunks: Vec<&[Box<dyn Array>]>,
) -> Result<Vec<i64>> {
    let arrs = chunks
        .iter()
        .enumerate()
        .map(|(idx, row)| {
            let arr = row[0]
                .as_any()
                .downcast_ref::<PrimitiveArray<T>>()
                .unwrap()
                .values_iter()
                .collect::<Vec<_>>();
            (idx, arr)
        })
        .collect::<Vec<_>>();

    let len = chunks.iter().map(|c| c[0].len()).sum();
    let mut out = Vec::with_capacity(len);
    let mut sort = BinaryHeap::<OneColMergeRow<T>>::with_capacity(len);
    for (stream, a) in arrs {
        for a in a.into_iter() {
            sort.push(OneColMergeRow(stream, *a));
        }
    }

    while let Some(OneColMergeRow(row_idx, _)) = sort.pop() {
        out.push(row_idx as i64);
    }

    Ok(out)
}

pub fn merge_two_primitives<T1: NativeType + Ord, T2: NativeType + Ord>(
    chunks: Vec<&[Box<dyn Array>]>,
    is_replacing: bool,
) -> Result<Vec<i64>> {
    let arrs = chunks
        .iter()
        .enumerate()
        .map(|(idx, row)| {
            let arr1 = row[0]
                .as_any()
                .downcast_ref::<PrimitiveArray<T1>>()
                .unwrap()
                .values_iter()
                .collect::<Vec<_>>();
            let arr2 = row[1]
                .as_any()
                .downcast_ref::<PrimitiveArray<T2>>()
                .unwrap()
                .values_iter()
                .collect::<Vec<_>>();
            (idx, arr1, arr2)
        })
        .collect::<Vec<_>>();

    let len = chunks.iter().map(|c| c[0].len()).sum();
    let mut out = Vec::with_capacity(len);
    let mut sort = BinaryHeap::<TwoColMergeRow<T1, T2>>::with_capacity(len);
    for (stream, a, b) in arrs {
        for (a, b) in a.into_iter().zip(b.into_iter()) {
            sort.push(TwoColMergeRow(stream, *a, *b));
        }
    }

    while let Some(TwoColMergeRow(row_idx, v1, _)) = sort.pop() {
        if is_replacing {
            if let Some(v) = sort.peek() {
                if v1 == v.1 {
                    out.push(-1);

                    continue;
                }
            }
        }

        out.push(row_idx as i64);
    }

    Ok(out)
}

// Merge chunks

// Merge multiple chunks into vector of MergedArrowChunk of arrays split by array_size

pub fn merge_chunks(
    chunks: Vec<&[Box<dyn Array>]>,
    index_cols: usize,
    is_replacing: bool,
) -> Result<Vec<i64>> {
    let res = match index_cols {
        1 => match chunks[0][0].data_type() {
            DataType::Int64 => merge_one_primitive::<i64>(chunks)?,
            _ => {
                return Err(StoreError::InvalidParameter(format!(
                    "merge not implemented for type {:?}",
                    chunks[0][0].data_type()
                )));
            }
        },
        2 => match (chunks[0][0].data_type(), chunks[0][0].data_type()) {
            (DataType::Int64, DataType::Int64) => {
                merge_two_primitives::<i64, i64>(chunks, is_replacing)?
            }
            _ => {
                return Err(StoreError::InvalidParameter(format!(
                    "merge not implemented for type {:?}",
                    chunks[0][0].data_type()
                )));
            }
        },
        _ => {
            return Err(StoreError::InvalidParameter(format!(
                "merge not implemented for {:?} columns",
                index_cols
            )));
        }
    };

    Ok(res)
}

pub fn check_intersection(
    chunks: &[ArrowChunk],
    other: Option<&ArrowChunk>,
    index_cols: usize,
    is_replacing: bool,
) -> bool {
    if other.is_none() {
        return false;
    }

    let mut iter = chunks.iter();
    let first = iter.next().unwrap();
    let offset = if is_replacing { 1 } else { 0 };
    let mut min_values = first.min_values()[..index_cols - offset].to_vec();
    let mut max_values = first.max_values()[..index_cols - offset].to_vec();
    for row in iter {
        if row.min_values <= min_values {
            min_values = row.min_values()[..index_cols - offset].to_vec();
        }
        if row.max_values >= max_values {
            max_values = row.max_values()[..index_cols - offset].to_vec();
        }
    }

    let other = other.unwrap();
    min_values <= other.max_values()[..index_cols - offset].to_vec()
        && max_values >= other.min_values()[..index_cols - offset].to_vec()
}

#[derive(Debug, Clone)]
pub struct ArrowChunk {
    pub stream: usize,
    chunk: Chunk<Box<dyn Array>>,
    min_values: Vec<KeyValue>,
    max_values: Vec<KeyValue>,
}

impl Eq for ArrowChunk {}

impl PartialOrd for ArrowChunk {
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

impl Ord for ArrowChunk {
    fn cmp(&self, other: &Self) -> Ordering {
        other.min_values.cmp(&self.min_values)
    }
}

impl PartialEq for ArrowChunk {
    fn eq(&self, other: &Self) -> bool {
        self.min_values == other.min_values && self.max_values == other.max_values
    }
}

impl ArrowChunk {
    pub fn new(chunk: Chunk<Box<dyn Array>>, stream: usize, index_cols: usize) -> Self {
        let (min_values, max_values) = chunk_min_max(&chunk, index_cols);

        Self {
            min_values,
            max_values,
            stream,
            chunk,
        }
    }

    pub fn min_values(&self) -> Vec<KeyValue> {
        self.min_values.clone()
    }

    pub fn max_values(&self) -> Vec<KeyValue> {
        self.max_values.clone()
    }
}

// this is a temporary array used to merge data pages avoiding downcasting

pub struct Options {
    pub index_cols: usize,
    pub is_replacing: bool,
    pub array_size: usize,
    pub chunk_size: usize,
    pub fields: Vec<String>,
}

pub struct MemChunkIterator {
    chunk: Option<Chunk<Box<dyn Array>>>,
}

impl MemChunkIterator {
    pub fn new(chunk: Option<Chunk<Box<dyn Array>>>) -> Self {
        Self { chunk }
    }
}

impl Iterator for MemChunkIterator {
    type Item = Result<Chunk<Box<dyn Array>>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.chunk.take().map(Ok)
    }
}

type SendableChunk = Box<dyn Iterator<Item=Result<Chunk<Box<dyn Array>>>> + Send>;

pub struct MergingIterator {
    // list of index cols (partitions) in parquet file
    index_cols: Vec<ColumnDescriptor>,
    is_replacing: bool,
    array_size: usize,
    schema: Schema,
    // list of streams to merge
    streams: Vec<SendableChunk>,
    // sorter for pages chunk from parquet
    sorter: BinaryHeap<ArrowChunk>,
    // pages chunk merge queue
    // merge result
    merge_result_buffer: VecDeque<Chunk<Box<dyn Array>>>,
}

impl MergingIterator {
    // Create new merger
    pub fn new(
        mut readers: Vec<BufReader<File>>,
        mem_chunk: Option<Chunk<Box<dyn Array>>>,
        schema: Schema,
        opts: Options,
    ) -> Result<Self> {
        let arrow_schemas = readers
            .iter_mut()
            .map(|r| arrow2::io::parquet::read::read_metadata(r).and_then(|md| infer_schema(&md)))
            .collect::<arrow2::error::Result<Vec<Schema>>>()?;
        // make unified schema
        let arrow_schema = try_merge_arrow_schemas(arrow_schemas)?;
        // make parquet schema
        let parquet_schema = to_parquet_schema(&arrow_schema)?;

        let index_cols = (0..opts.index_cols)
            .map(|idx| parquet_schema.columns()[idx].to_owned())
            .collect::<Vec<_>>();

        let mut arrow_streams = readers
            .into_iter()
            .map(|v| {
                Box::new(ArrowIteratorImpl::new(v, opts.fields.clone(), opts.chunk_size).unwrap())
                    as Box<dyn Iterator<Item=Result<Chunk<Box<dyn Array>>>> + Send>
            })
            .collect::<Vec<_>>();

        if mem_chunk.is_some() {
            arrow_streams.push(Box::new(MemChunkIterator::new(mem_chunk))
                as Box<dyn Iterator<Item=Result<Chunk<Box<dyn Array>>>> + Send>);
        }

        let mut mr = Self {
            index_cols,
            is_replacing: opts.is_replacing,
            array_size: opts.array_size,
            schema,
            streams: arrow_streams,
            sorter: BinaryHeap::new(),
            merge_result_buffer: VecDeque::with_capacity(10),
        };
        for stream_id in 0..mr.streams.len() {
            if let Some(chunk) = mr.next_stream_chunk(stream_id)? {
                mr.sorter.push(chunk);
            }
        }
        Ok(mr)
    }

    fn merge_queue(&self, queue: &[&Chunk<Box<dyn Array>>]) -> Result<Vec<Chunk<Box<dyn Array>>>> {
        counter!("store.scan_merges_total").increment(1);
        let arrs = queue
            .iter()
            .map(|chunk| chunk.columns())
            .collect::<Vec<_>>();
        let reorder = merge_chunks(arrs, self.index_cols.len(), self.is_replacing)?;

        let cols_len = queue[0].columns().len();
        let mut arrs = (0..cols_len)
            .map(|col_id| {
                let arrs = queue
                    .iter()
                    .map(|chunk| chunk.columns()[col_id].as_ref())
                    .collect::<Vec<_>>();
                let mut arr_cursors = vec![0; arrs.len()];
                let mut growable = make_growable(&arrs, false, reorder.len());
                for idx in reorder.iter() {
                    if *idx == -1 {
                        continue;
                    }
                    growable.extend(*idx as usize, arr_cursors[*idx as usize], 1);
                    arr_cursors[*idx as usize] += 1;
                }

                growable.as_box()
            })
            .collect::<Vec<_>>();

        let mut out = vec![];

        let mut len = arrs[0].len();
        for i in (0..reorder.len()).step_by(self.array_size) {
            let arrs = arrs
                .iter_mut()
                .map(|arr| arr.sliced(i, min(self.array_size, len)))
                .collect::<Vec<_>>();

            out.push(Chunk::new(arrs));
            len -= min(self.array_size, len);
        }
        Ok(out)
    }

    fn next_stream_chunk(&mut self, stream_id: usize) -> Result<Option<ArrowChunk>> {
        let maybe_chunk = self.streams[stream_id].next();
        if maybe_chunk.is_none() {
            return Ok(None);
        }

        let chunk = add_null_cols_to_chunk(&maybe_chunk.unwrap()?, &self.schema)?;
        Ok(Some(ArrowChunk::new(
            chunk,
            stream_id,
            self.index_cols.len(),
        )))
    }
}

fn add_null_cols_to_chunk(
    chunk: &Chunk<Box<dyn Array>>,
    schema: &Schema,
) -> Result<Chunk<Box<dyn Array>>> {
    let mut cols = chunk.columns().to_vec();
    let mut null_cols = vec![];
    for (idx, field) in schema.fields.iter().enumerate() {
        if idx >= cols.len() {
            null_cols.push(new_null_array(field.data_type.clone(), chunk.len()));
        }
    }
    cols.append(&mut null_cols);
    Ok(Chunk::new(cols))
}

impl Iterator for MergingIterator {
    type Item = Result<Chunk<Box<dyn Array>>>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(chunk) = self.merge_result_buffer.pop_front() {
            return Some(Ok(chunk));
        }
        let mut merge_queue: Vec<ArrowChunk> = vec![];
        if let Some(chunk) = self.sorter.pop() {
            if let Some(chunk) = self.next_stream_chunk(chunk.stream).ok()? {
                // return Some(Ok(chunk.chunk.clone()));
                self.sorter.push(chunk);
            }
            merge_queue.push(chunk);
        } else {
            return None;
        };

        while check_intersection(
            &merge_queue,
            self.sorter.peek(),
            self.index_cols.len(),
            self.is_replacing,
        ) {
            // in case of intersection, take chunk and add it to merge queue
            let next = self.sorter.pop().unwrap();
            // try to take next chunk of stream and add it to sorter
            if let Some(chunk) = self.next_stream_chunk(next.stream).ok()? {
                self.sorter.push(chunk);
            }
            // push chunk to merge queue
            merge_queue.push(next);
        }
        // check queue len. Queue len may be 1 if there is no intersection
        if merge_queue.len() > 1 {
            // in case of intersection, merge queue
            let res = self
                .merge_queue(
                    &merge_queue
                        .iter()
                        .map(|chunk| &chunk.chunk)
                        .collect::<Vec<_>>(),
                )
                .ok()?;
            // todo split arrays
            self.merge_result_buffer = VecDeque::from(res);

            self.next()
        } else {
            // queue contains only one chunk, so we can just push it to result
            let chunk = merge_queue.pop().unwrap();

            Some(Ok(chunk.chunk))
        }
    }
}
// #[cfg(test)]
// mod tests {
// use std::io::Cursor;
// use std::sync::Arc;
//
// use arrow2::array::PrimitiveArray;
// use arrow2::chunk::Chunk;
// use arrow2::datatypes::DataType;
// use arrow2::datatypes::Field;
// use arrow2::io::parquet::write::RowGroupIterator;
// use arrow2::io::parquet::write::WriteOptions;
// use parquet2::compression::CompressionOptions;
// use parquet2::write::Version;
//
// use crate::parquet::arrow_merger::MergingIterator;
// use crate::parquet::arrow_merger::Options;
// use crate::test_util::create_parquet_from_chunk;
// use crate::test_util::parse_markdown_tables;
//
// #[test]
// fn it_works() {
// let v = (0..100).collect::<Vec<_>>();
// let cols = vec![
// vec![
// PrimitiveArray::<i64>::from_slice(v.clone()).boxed(),
// PrimitiveArray::<i64>::from_slice(v.clone()).boxed(),
// PrimitiveArray::<i64>::from_slice(v.clone()).boxed(),
// ],
// vec![
// PrimitiveArray::<i64>::from_slice(v.clone()).boxed(),
// PrimitiveArray::<i64>::from_slice(v.clone()).boxed(),
// ],
// vec![
// PrimitiveArray::<i64>::from_slice(v.clone()).boxed(),
// PrimitiveArray::<i64>::from_slice(v.clone()).boxed(),
// PrimitiveArray::<i64>::from_slice(v).boxed(),
// ],
// ];
//
// let fields = vec![
// vec![
// Field::new("f1", DataType::Int64, false),
// Field::new("f2", DataType::Int64, false),
// Field::new("f3", DataType::Int64, false),
// ],
// vec![
// Field::new("f1", DataType::Int64, false),
// Field::new("f2", DataType::Int64, false),
// ],
// vec![
// Field::new("f1", DataType::Int64, false),
// Field::new("f2", DataType::Int64, false),
// Field::new("f3", DataType::Int64, false),
// ],
// ];
//
// let readers = cols
// .into_iter()
// .zip(fields.iter())
// .enumerate()
// .map(|(idx, (cols, fields))| {
// let chunk = Chunk::new(cols);
// let mut w = Cursor::new(vec![]);
// create_parquet_from_chunk(
// chunk,
// fields.to_owned(),
// &mut w,
// Some(idx * 10 + 10),
// 10,
// )
// .unwrap();
//
// w
// })
// .collect::<Vec<_>>();
//
// let opts = Options {
// index_cols: 1,
// array_size: 9,
// chunk_size: 10,
// fields: vec!["f1".to_string(), "f2".to_string(), "f3".to_string()],
// };
// let mut merger = MergingIterator::new(readers, None, opts).unwrap();
// while let Some(chunk) = merger.next() {
// println!("{:#?}", chunk.unwrap());
// }
// }
// }
