use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::fs;
use std::fs::File;
use std::io::{BufReader, Read};
use std::io::Seek;
use std::io::Write;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

use arrow2::array::new_null_array;
use arrow2::array::Array;
use arrow2::array::BinaryArray;
use arrow2::array::BooleanArray;
use arrow2::array::Float32Array;
use arrow2::array::Float64Array;
use arrow2::array::growable::make_growable;
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
use arrow2::compute::merge_sort::{merge_sort, merge_sort_slices};
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
use crate::{merge_arrays};
use crate::merge_arrays_inner;
use crate::merge_list_arrays;
use crate::merge_list_arrays_inner;
use crate::merge_list_primitive_arrays;
use crate::merge_primitive_arrays;
use crate::parquet::merger;
use crate::parquet::merger::arrow::{merge_chunks, merge_two_primitives, merge_two_primitives2};
use crate::parquet::merger::arrow::try_merge_schemas;
use crate::parquet::merger::parquet::{ArrowIterator, CompressedPageIterator, IndexChunk, Value};
use crate::parquet::merger::parquet::data_page_to_array;
use crate::parquet::merger::parquet::ColumnPath;
use crate::parquet::merger::{MergeReorder, TmpArray};


pub fn check_intersection(chunks: &[ArrowChunk], other: Option<&ArrowChunk>) -> bool {
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

struct ArrowChunk {
    pub stream: usize,
    chunk: Chunk<Box<dyn Array>>,
    min_values: Vec<Value>,
    max_values: Vec<Value>,
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
        let (min_values, max_values) = (0..index_cols)
            .into_iter()
            .map(|id| {
                let arr = chunk.columns()[id].as_ref();
                match arr.data_type() {
                    // todo move to macro
                    DataType::Int32 => {
                        let arr = arr.as_any().downcast_ref::<Int32Array>().unwrap();
                        let min = Value::from(arr.value(0));
                        let max = Value::from(arr.value(arr.len() - 1));
                        (min, max)
                    }
                    DataType::Int64 => {
                        let arr = arr.as_any().downcast_ref::<Int64Array>().unwrap();
                        let min = Value::from(arr.value(0));
                        let max = Value::from(arr.value(arr.len() - 1));
                        (min, max)
                    }
                    _ => unimplemented!("only support int32 and int64")
                }
            })
            .unzip();

        Self {
            min_values,
            max_values,
            stream,
            chunk,
        }
    }

    pub fn min_values(&self) -> Vec<Value> {
        self.min_values.clone()
    }

    pub fn max_values(&self) -> Vec<Value> {
        self.max_values.clone()
    }

    // Get length of arrow chunk
    pub fn len(&self) -> usize {
        self.chunk.columns()[0].len()
    }

    // Check if chunk is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[derive(Debug)]
pub struct Merged(pub Vec<Box<dyn Array>>, pub MergeReorder);

impl Merged {
    pub fn new(chunk: Vec<Box<dyn Array>>, merge_reorder: MergeReorder) -> Self {
        Self(chunk, merge_reorder)
    }

    pub fn num_values(&self) -> usize {
        self.0.len()
    }
}

// this is a temporary array used to merge data pages avoiding downcasting

pub struct Options {
    pub index_cols: usize,
    pub fields: Vec<String>,
}

pub struct Merger<R>
    where
        R: Read + Seek,
{
    // list of index cols (partitions) in parquet file
    index_cols: Vec<ColumnDescriptor>,
    // final schema of parquet file (merged multiple schemas)
    parquet_schema: SchemaDescriptor,
    // final arrow schema
    arrow_schema: Schema,
    // list of streams to merge
    streams: Vec<ArrowIterator<R>>,
    // temporary array for merging data pages
    // this is used to avoid downcast on each row iteration. See `merge_arrays`
    tmp_arrays: Vec<HashMap<ColumnPath, TmpArray>>,
    // indices of temp arrays
    tmp_array_idx: Vec<HashMap<ColumnPath, usize>>,
    // sorter for pages chunk from parquet
    sorter: BinaryHeap<ArrowChunk>,
    // pages chunk merge queue
    // merge result
    merge_result_buffer: VecDeque<Chunk<Box<dyn Array>>>,
}

struct MergeIterator {}

impl Iterator for MergeIterator {
    type Item = Result<Chunk<Box<dyn Array>>>;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

impl<R> Merger<R>
    where
        R: Read + Seek,
{
    // Create new merger
    pub fn new(mut readers: Vec<R>,
               opts: Options) -> Result<Self> {
        let arrow_schemas = readers
            .iter_mut()
            .map(|r| arrow2::io::parquet::read::read_metadata(r).and_then(|md| infer_schema(&md)))
            .collect::<arrow2::error::Result<Vec<Schema>>>()?;
        // make unified schema
        let arrow_schema = try_merge_schemas(arrow_schemas)?;
        // make parquet schema
        let parquet_schema = to_parquet_schema(&arrow_schema)?;
        let page_streams = readers
            .into_iter()
            // todo make dynamic page size
            .map(|r| CompressedPageIterator::try_new(r, 1024 * 1024))
            .collect::<Result<Vec<_>>>()?;
        // initialize parquet streams/readers

        let index_cols = (0..opts.index_cols)
            .map(|idx| parquet_schema.columns()[idx].to_owned())
            .collect::<Vec<_>>();

        let fields = opts.fields.iter().map(|f| parquet_schema.columns().iter().find(|c| c.path_in_schema[0] == *f).unwrap().to_owned()).collect::<Vec<_>>();
        let arrow_streams = page_streams
            .into_iter()
            .map(|v| ArrowIterator::new(v, fields.clone()))
            .collect::<Vec<_>>();
        let streams_n = arrow_streams.len();


        let mut mr = Self {
            index_cols,
            parquet_schema,
            arrow_schema,
            streams: arrow_streams,
            tmp_arrays: (0..streams_n).map(|_| HashMap::new()).collect(),
            tmp_array_idx: (0..streams_n).map(|_| HashMap::new()).collect(),
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

    fn merge_queue(&self, queue: &Vec<&Chunk<Box<dyn Array>>>) -> Result<Chunk<Box<dyn Array>>> {
        let arrs = queue.iter().map(|chunk| chunk.columns()).collect::<Vec<_>>();
        let reorder = merge_two_primitives2::<i64, i64>(arrs)?;

        let cols_len = queue[0].columns().len();
        let arrs = (0..cols_len).into_iter().map(|col_id| {
            let mut arrs = queue.iter().map(|chunk| chunk.columns()[col_id].as_ref()).collect::<Vec<_>>();
            let mut arr_cursors = vec![0; arrs.len()];
            let mut growable = make_growable(&arrs, false, reorder.len());
            for idx in reorder.iter() {
                growable.extend(*idx, arr_cursors[*idx], 1);
                arr_cursors[*idx] += 1;
            }

            growable.as_box()
        }).collect::<Vec<_>>();

        Ok(Chunk::new(arrs))
    }

    fn next_stream_chunk(&mut self, stream_id: usize) -> Result<Option<ArrowChunk>> {
        let maybe_chunk = self.streams[stream_id].next()?;
        if maybe_chunk.is_none() {
            return Ok(None);
        }

        Ok(Some(ArrowChunk::new(maybe_chunk.unwrap(), stream_id, self.index_cols.len())))
    }
}

impl<R: Read + Seek> Iterator for Merger<R> {
    type Item = Result<Chunk<Box<dyn Array>>>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(chunk) = self.merge_result_buffer.pop_front() {
            return Some(Ok(chunk));
        }

        let mut merge_queue: Vec<ArrowChunk> = vec![];
        while let Some(chunk) = self.sorter.pop() {
            if let Some(chunk) = self.next_stream_chunk(chunk.stream).ok()? {
                self.sorter.push(chunk);
            }

            merge_queue.push(chunk);

            while check_intersection(&merge_queue, self.sorter.peek()) {
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
                let res = self.merge_queue(&merge_queue.iter().map(|chunk| &chunk.chunk).collect::<Vec<_>>()).ok()?;
                // todo split arrays
                self.merge_result_buffer = VecDeque::from(vec![res]);
            } else {
                // queue contains only one chunk, so we can just push it to result
                let chunk = merge_queue.pop().unwrap();
                return Some(Ok(chunk.chunk));
            }
        }

        return self.merge_result_buffer.pop_front().map(|v| Ok(v));


        None
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::sync::Arc;
    use arrow2::array::PrimitiveArray;
    use arrow2::chunk::Chunk;
    use arrow2::datatypes::{DataType, Field};
    use arrow2::io::parquet::write::{RowGroupIterator, WriteOptions};
    use parquet2::compression::CompressionOptions;
    use parquet2::write::Version;
    use crate::parquet::merger::arrow_merger::{Merger, Options};
    use crate::test_util::{create_parquet_from_chunk, parse_markdown_tables};


    #[test]
    fn it_works() {
        let v = (0..100).collect::<Vec<_>>();
        let cols = vec![
            vec![
                PrimitiveArray::<i64>::from_slice(v.clone()).boxed(),
                PrimitiveArray::<i64>::from_slice(v.clone()).boxed(),
                PrimitiveArray::<i64>::from_slice(v.clone()).boxed(),
            ],
            vec![
                PrimitiveArray::<i64>::from_slice(v.clone()).boxed(),
                PrimitiveArray::<i64>::from_slice(v.clone()).boxed(),
                PrimitiveArray::<i64>::from_slice(v.clone()).boxed(),
            ],
            vec![
                PrimitiveArray::<i64>::from_slice(v.clone()).boxed(),
                PrimitiveArray::<i64>::from_slice(v.clone()).boxed(),
                PrimitiveArray::<i64>::from_slice(v).boxed(),
            ],
        ];

        let fields = vec![
            vec![
                Field::new("f1", DataType::Int64, false),
                Field::new("f2", DataType::Int64, false),
                Field::new("f3", DataType::Int64, false),
            ],
            vec![
                Field::new("f1", DataType::Int64, false),
                Field::new("f2", DataType::Int64, false),
                Field::new("f3", DataType::Int64, false),
            ],
            vec![
                Field::new("f1", DataType::Int64, false),
                Field::new("f2", DataType::Int64, false),
                Field::new("f3", DataType::Int64, false),
            ],
        ];

        let readers = cols
            .into_iter()
            .zip(fields.iter()).enumerate()
            .map(|(idx, (cols, fields))| {
                let chunk = Chunk::new(cols);
                let mut w = Cursor::new(vec![]);
                create_parquet_from_chunk(chunk, fields.to_owned(), &mut w, Some(idx * 10 + 10), 10).unwrap();

                w
            })
            .collect::<Vec<_>>();

        let opts = Options {
            index_cols: 1,
            fields: vec!["f1".to_string(), "f2".to_string(), "f3".to_string()],
        };
        let mut merger = Merger::new(readers, opts).unwrap();
        while let Some(chunk) = merger.next()
        {
            println!("{:#?}", chunk.unwrap());
        }
    }
}