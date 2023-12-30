use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::fs;
use std::fs::File;
use std::io::BufReader;
use std::io::BufWriter;
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
use arrow2::array::PrimitiveArray;
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
use arrow2::io::parquet::write::array_to_columns;
use arrow2::io::parquet::write::to_parquet_schema;
use arrow2::io::parquet::write::WriteOptions as ArrowWriteOptions;
use arrow2::offset::OffsetsBuffer;
use arrow2::types::NativeType;
use metrics::gauge;
use metrics::histogram;
use num_traits::ToPrimitive;
use parquet2::compression::CompressionOptions;
use parquet2::encoding::Encoding;
use parquet2::metadata::ColumnDescriptor;
use parquet2::metadata::SchemaDescriptor;
use parquet2::page::CompressedPage;
use parquet2::write::compress;
use parquet2::write::FileSeqWriter;
use parquet2::write::Version;
use parquet2::write::WriteOptions;

use crate::error::Result;
use crate::merge_arrays;
use crate::merge_arrays_inner;
use crate::merge_list_arrays;
use crate::merge_list_arrays_inner;
use crate::merge_list_primitive_arrays;
use crate::merge_primitive_arrays;
use crate::parquet::merger::parquet::array_to_pages_simple;
use crate::parquet::merger::parquet::data_page_to_array;
use crate::parquet::merger::parquet::get_page_min_max_values;
use crate::parquet::merger::parquet::pages_to_arrays;
use crate::parquet::merger::parquet::ColumnPath;
use crate::parquet::merger::parquet::CompressedPageIterator;
use crate::parquet::merger::parquet::ParquetValue;
use crate::parquet::merger::try_merge_arrow_schemas;
use crate::parquet::merger::IndexChunk;
use crate::parquet::merger::OneColMergeRow;
use crate::parquet::merger::ThreeColMergeRow;
use crate::parquet::merger::TmpArray;
use crate::parquet::merger::TwoColMergeRow;
use crate::KeyValue;

#[derive(Debug)]
// Arrow chunk before being merged
// Arrow chunk is an unpacked CompressedPage
pub struct ArrowIndexChunk {
    // stream of arrow chunk Used to identify the chunk during merge
    pub stream: usize,
    pub cols: Vec<Box<dyn Array>>,
    pub min_values: Vec<ParquetValue>,
    pub max_values: Vec<ParquetValue>,
}

impl Eq for ArrowIndexChunk {}

impl PartialOrd for ArrowIndexChunk {
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

impl Ord for ArrowIndexChunk {
    fn cmp(&self, other: &Self) -> Ordering {
        other.min_values.cmp(&self.min_values)
    }
}

impl PartialEq for ArrowIndexChunk {
    fn eq(&self, other: &Self) -> bool {
        self.min_values == other.min_values && self.max_values == other.max_values
    }
}

impl ArrowIndexChunk {
    pub fn new(cols: Vec<Box<dyn Array>>, stream: usize) -> Self {
        let (min_values, max_values) = cols
            .iter()
            .map(|arr| {
                match arr.data_type() {
                    // todo move to macro
                    DataType::Int8 => {
                        let arr = arr.as_any().downcast_ref::<Int8Array>().unwrap();
                        let min = ParquetValue::from(arr.value(0));
                        let max = ParquetValue::from(arr.value(arr.len() - 1));
                        (min, max)
                    }
                    DataType::Int16 => {
                        let arr = arr.as_any().downcast_ref::<Int16Array>().unwrap();
                        let min = ParquetValue::from(arr.value(0));
                        let max = ParquetValue::from(arr.value(arr.len() - 1));
                        (min, max)
                    }
                    DataType::Int32 => {
                        let arr = arr.as_any().downcast_ref::<Int32Array>().unwrap();
                        let min = ParquetValue::from(arr.value(0));
                        let max = ParquetValue::from(arr.value(arr.len() - 1));
                        (min, max)
                    }
                    DataType::Int64 => {
                        let arr = arr.as_any().downcast_ref::<Int64Array>().unwrap();
                        let min = ParquetValue::from(arr.value(0));
                        let max = ParquetValue::from(arr.value(arr.len() - 1));
                        (min, max)
                    }
                    DataType::Timestamp(_, _) => {
                        let arr = arr.as_any().downcast_ref::<Int64Array>().unwrap();
                        let min = ParquetValue::from(arr.value(0));
                        let max = ParquetValue::from(arr.value(arr.len() - 1));
                        (min, max)
                    }
                    _ => unimplemented!("unsupported type {:?}", arr.data_type()),
                }
            })
            .unzip();

        Self {
            cols,
            min_values,
            max_values,
            stream,
        }
    }

    pub fn min_values(&self) -> Vec<ParquetValue> {
        self.min_values.clone()
    }

    pub fn max_values(&self) -> Vec<ParquetValue> {
        self.max_values.clone()
    }

    // Get length of arrow chunk
    pub fn len(&self) -> usize {
        self.cols[0].len()
    }

    // Check if chunk is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub fn check_intersection(chunks: &[PagesIndexChunk], other: Option<&PagesIndexChunk>) -> bool {
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

#[derive(Debug)]
pub struct PagesIndexChunk {
    pub cols: Vec<Vec<CompressedPage>>,
    pub min_values: Vec<ParquetValue>,
    pub max_values: Vec<ParquetValue>,
    pub stream: usize,
}

impl Eq for PagesIndexChunk {}

impl PartialOrd for PagesIndexChunk {
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

impl Ord for PagesIndexChunk {
    fn cmp(&self, other: &Self) -> Ordering {
        other.min_values.cmp(&self.min_values)
    }
}

impl PartialEq for PagesIndexChunk {
    fn eq(&self, other: &Self) -> bool {
        self.min_values == other.min_values && self.max_values == other.max_values
    }
}

impl IndexChunk for PagesIndexChunk {
    fn min_values(&self) -> Vec<ParquetValue> {
        self.min_values.clone()
    }

    fn max_values(&self) -> Vec<ParquetValue> {
        self.max_values.clone()
    }
}

impl PagesIndexChunk {
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

    pub fn len(&self) -> usize {
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
    ) -> Result<ArrowIndexChunk> {
        let cols = self
            .cols
            .into_iter()
            .zip(index_cols.iter())
            .map(|(pages, cd)| {
                pages_to_arrays(pages, cd, None, buf).map(|mut res| res.pop().unwrap())
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(ArrowIndexChunk::new(cols, self.stream))
    }

    pub fn from_arrow(arrs: &[Box<dyn Array>], index_cols: &[ColumnDescriptor]) -> Result<Self> {
        let opts = ArrowWriteOptions {
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

#[derive(Debug)]
pub struct MergedPagesChunk(pub PagesIndexChunk, pub MergeReorder);

impl MergedPagesChunk {
    pub fn new(chunk: PagesIndexChunk, merge_reorder: MergeReorder) -> Self {
        Self(chunk, merge_reorder)
    }

    pub fn num_values(&self) -> usize {
        self.0.len()
    }
}

#[derive(Debug)]
// Arrow chunk after being merged
pub struct MergedArrowChunk {
    pub cols: Vec<Box<dyn Array>>,
    // contains the stream id of each row so we can take the rows in the correct order
    pub reorder: Vec<usize>,
}

impl MergedArrowChunk {
    // Create new merged arrow chunk
    pub fn new(arrs: Vec<Box<dyn Array>>, reorder: Vec<usize>) -> Self {
        Self {
            cols: arrs,
            reorder,
        }
    }
}

// array_size - size of output arrays
pub fn merge_one_primitive<T: NativeType + Ord>(
    chunks: Vec<ArrowIndexChunk>,
    array_size: usize,
) -> Result<Vec<MergedArrowChunk>> {
    // downcast to primitive array
    let mut arr_iters = chunks
        .iter()
        .map(|row| {
            row.cols[0]
                .as_any()
                .downcast_ref::<PrimitiveArray<T>>()
                .unwrap()
                .values_iter()
        })
        .collect::<Vec<_>>();

    // use binary heap for sorting
    let mut sort = BinaryHeap::<OneColMergeRow<T>>::with_capacity(array_size);

    let mut res = vec![];
    // create buffers
    // perf: use predefined reusable buffers
    let mut out_col = Vec::with_capacity(array_size);
    let mut order = Vec::with_capacity(array_size);

    // push all the values in sorter
    for (row_id, iters) in arr_iters.iter_mut().enumerate().take(chunks.len()) {
        let mr = OneColMergeRow(row_id, *iters.next().unwrap());
        sort.push(mr);
    }

    // get sorted values
    while let Some(OneColMergeRow(row_idx, v)) = sort.pop() {
        out_col.push(v);
        order.push(chunks[row_idx].stream);
        if let Some(v) = arr_iters[row_idx].next() {
            let mr = OneColMergeRow(row_idx, *v);
            sort.push(mr);
        }

        // limit output by array size
        if out_col.len() >= array_size {
            let out = vec![PrimitiveArray::<T>::from_vec(out_col.drain(..).collect()).boxed()];
            let arr_order = order.drain(..).collect();
            res.push(MergedArrowChunk::new(out, arr_order));
        }
    }

    // drain the rest
    if !out_col.is_empty() {
        let out = vec![PrimitiveArray::<T>::from_vec(out_col.drain(..).collect()).boxed()];
        let arr_order = order.drain(..).collect();
        res.push(MergedArrowChunk::new(out, arr_order));
    }

    Ok(res)
}

type Slice = (usize, usize, usize);

// merge chunks with two column partition that are primitives

// array_size - size of output arrays
pub fn merge_two_primitives<T1: NativeType + Ord, T2: NativeType + Ord>(
    chunks: Vec<ArrowIndexChunk>,
    array_size: usize,
) -> Result<Vec<MergedArrowChunk>> {
    let mut arr_iters = chunks
        .iter()
        .map(|row| {
            let arr1 = row.cols[0]
                .as_any()
                .downcast_ref::<PrimitiveArray<T1>>()
                .unwrap()
                .values_iter();
            let arr2 = row.cols[1]
                .as_any()
                .downcast_ref::<PrimitiveArray<T2>>()
                .unwrap()
                .values_iter();
            (arr1, arr2)
        })
        .collect::<Vec<_>>();

    let mut sort = BinaryHeap::<TwoColMergeRow<T1, T2>>::with_capacity(array_size);

    let mut res = vec![];
    let mut out_col1 = Vec::with_capacity(array_size);
    let mut out_col2 = Vec::with_capacity(array_size);
    let mut order = Vec::with_capacity(array_size);

    for (row_id, iter) in arr_iters.iter_mut().enumerate().take(chunks.len()) {
        let mr = TwoColMergeRow(row_id, *iter.0.next().unwrap(), *iter.1.next().unwrap());
        sort.push(mr);
    }

    while let Some(TwoColMergeRow(row_idx, v1, v2)) = sort.pop() {
        out_col1.push(v1);
        out_col2.push(v2);
        order.push(chunks[row_idx].stream);
        if let Some(v1) = arr_iters[row_idx].0.next() {
            let v2 = arr_iters[row_idx].1.next().unwrap();
            let mr = TwoColMergeRow(row_idx, *v1, *v2);
            sort.push(mr);
        }

        if out_col1.len() >= array_size {
            let out = vec![
                PrimitiveArray::<T1>::from_vec(out_col1.drain(..).collect()).boxed(),
                PrimitiveArray::<T2>::from_vec(out_col2.drain(..).collect()).boxed(),
            ];
            let arr_order = order.drain(..).collect();
            res.push(MergedArrowChunk::new(out, arr_order));
        }
    }

    if !out_col1.is_empty() {
        let out = vec![
            PrimitiveArray::<T1>::from_vec(out_col1.drain(..).collect()).boxed(),
            PrimitiveArray::<T2>::from_vec(out_col2.drain(..).collect()).boxed(),
        ];
        let arr_order = order.drain(..).collect();
        res.push(MergedArrowChunk::new(out, arr_order));
    }

    Ok(res)
}

// array_size - size of output arrays
// todo move to macro?
pub fn merge_three_primitives<T1: NativeType + Ord, T2: NativeType + Ord, T3: NativeType + Ord>(
    chunks: Vec<ArrowIndexChunk>,
    array_size: usize,
) -> Result<Vec<MergedArrowChunk>> {
    let mut arr_iters = chunks
        .iter()
        .map(|row| {
            let arr1 = row.cols[0]
                .as_any()
                .downcast_ref::<PrimitiveArray<T1>>()
                .unwrap()
                .values_iter();
            let arr2 = row.cols[1]
                .as_any()
                .downcast_ref::<PrimitiveArray<T2>>()
                .unwrap()
                .values_iter();
            let arr3 = row.cols[2]
                .as_any()
                .downcast_ref::<PrimitiveArray<T3>>()
                .unwrap()
                .values_iter();
            (arr1, arr2, arr3)
        })
        .collect::<Vec<_>>();

    let mut sort = BinaryHeap::<ThreeColMergeRow<T1, T2, T3>>::with_capacity(array_size);

    let mut res = vec![];
    let mut out_col1 = Vec::with_capacity(array_size);
    let mut out_col2 = Vec::with_capacity(array_size);
    let mut out_col3 = Vec::with_capacity(array_size);
    let mut order = Vec::with_capacity(array_size);

    for (row_id, iter) in arr_iters.iter_mut().enumerate().take(chunks.len()) {
        let mr = ThreeColMergeRow(
            row_id,
            *iter.0.next().unwrap(),
            *iter.1.next().unwrap(),
            *iter.2.next().unwrap(),
        );
        sort.push(mr);
    }

    while let Some(ThreeColMergeRow(row_idx, v1, v2, v3)) = sort.pop() {
        out_col1.push(v1);
        out_col2.push(v2);
        out_col3.push(v3);
        order.push(chunks[row_idx].stream);
        if let Some(v1) = arr_iters[row_idx].0.next() {
            let v2 = arr_iters[row_idx].1.next().unwrap();
            let v3 = arr_iters[row_idx].2.next().unwrap();
            let mr = ThreeColMergeRow(row_idx, *v1, *v2, *v3);
            sort.push(mr);
        }

        if out_col1.len() >= array_size {
            let out = vec![
                PrimitiveArray::<T1>::from_vec(out_col1.drain(..).collect()).boxed(),
                PrimitiveArray::<T2>::from_vec(out_col2.drain(..).collect()).boxed(),
                PrimitiveArray::<T3>::from_vec(out_col3.drain(..).collect()).boxed(),
            ];
            let arr_order = order.drain(..).collect();
            res.push(MergedArrowChunk::new(out, arr_order));
        }
    }

    if !out_col1.is_empty() {
        let out = vec![
            PrimitiveArray::<T1>::from_vec(out_col1.drain(..).collect()).boxed(),
            PrimitiveArray::<T2>::from_vec(out_col2.drain(..).collect()).boxed(),
            PrimitiveArray::<T3>::from_vec(out_col3.drain(..).collect()).boxed(),
        ];
        let arr_order = order.drain(..).collect();
        res.push(MergedArrowChunk::new(out, arr_order));
    }

    Ok(res)
}

// Merge chunks

// Merge multiple chunks into vector of MergedArrowChunk of arrays split by array_size

// panics if merges is not implemented for combination of types
pub fn merge_chunks(
    chunks: Vec<ArrowIndexChunk>,
    array_size: usize,
) -> Result<Vec<MergedArrowChunk>> {
    // supported lengths (count of index columns)
    match chunks[0].cols.len() {
        1 => match chunks[0].cols[0].data_type().to_physical_type() {
            arrow2::datatypes::PhysicalType::Primitive(pt) => match pt {
                ArrowPrimitiveType::Int64 => merge_one_primitive::<i64>(chunks, array_size),
                _ => unimplemented!("merge is not implemented for {pt:?} primitive type"),
            },
            _ => unimplemented!(
                "merge not implemented for {:?} type",
                chunks[0].cols[0].data_type()
            ),
        },
        2 => {
            match (
                chunks[0].cols[0].data_type().to_physical_type(),
                chunks[0].cols[1].data_type().to_physical_type(),
            ) {
                (
                    arrow2::datatypes::PhysicalType::Primitive(a),
                    arrow2::datatypes::PhysicalType::Primitive(b),
                ) => {
                    match (a, b) {
                        // Put here possible combination that you need
                        // Or find a way to merge any types dynamically without performance penalty
                        (ArrowPrimitiveType::Int64, ArrowPrimitiveType::Int64) => {
                            merge_two_primitives::<i64, i64>(chunks, array_size)
                        }
                        (ArrowPrimitiveType::Int64, ArrowPrimitiveType::Int32) => {
                            merge_two_primitives::<i64, i32>(chunks, array_size)
                        }
                        _ => unimplemented!(
                            "merge is not implemented for {a:?} {b:?} primitive types"
                        ),
                    }
                }
                _ => unimplemented!(
                    "merge not implemented for {:?} {:?} types",
                    chunks[0].cols[0].data_type(),
                    chunks[0].cols[1].data_type()
                ),
            }
        }
        3 => {
            match (
                chunks[0].cols[0].data_type().to_physical_type(),
                chunks[0].cols[1].data_type().to_physical_type(),
                chunks[0].cols[2].data_type().to_physical_type(),
            ) {
                (
                    arrow2::datatypes::PhysicalType::Primitive(a),
                    arrow2::datatypes::PhysicalType::Primitive(b),
                    arrow2::datatypes::PhysicalType::Primitive(c),
                ) => {
                    match (a, b, c) {
                        // Put here possible combination that you need
                        // Or find a way to merge any types dynamically without performance penalty
                        (
                            ArrowPrimitiveType::Int64,
                            ArrowPrimitiveType::Int64,
                            ArrowPrimitiveType::Int64,
                        ) => merge_three_primitives::<i64, i64, i64>(chunks, array_size),
                        _ => unimplemented!(
                            "merge is not implemented for {a:?} {b:?} primitive types"
                        ),
                    }
                }
                _ => unimplemented!(
                    "merge not implemented for {:?} {:?} types",
                    chunks[0].cols[0].data_type(),
                    chunks[0].cols[1].data_type()
                ),
            }
        }
        _ => unimplemented!(
            "merge not implemented for {:?} columns",
            chunks[0].cols.len()
        ),
    }
}

pub struct Options {
    pub index_cols: usize,
    pub data_page_size_limit_bytes: Option<usize>,
    pub row_group_values_limit: usize,
    pub array_page_size: usize,
    pub out_part_id: usize,
    pub merge_max_page_size: usize,
    pub max_part_size_bytes: Option<usize>,
}

pub struct Merger<R>
where R: Read
{
    // list of index cols (partitions) in parquet file
    index_cols: Vec<ColumnDescriptor>,
    // final schema of parquet file (merged multiple schemas)
    parquet_schema: SchemaDescriptor,
    // final arrow schema
    arrow_schema: Schema,
    // list of streams to merge
    page_streams: Vec<CompressedPageIterator<R>>,
    // temporary array for merging data pages
    // this is used to avoid downcast on each row iteration. See `merge_arrays`
    tmp_arrays: Vec<HashMap<ColumnPath, TmpArray>>,
    // indices of temp arrays
    tmp_array_idx: Vec<HashMap<ColumnPath, usize>>,
    // sorter for pages chunk from parquet
    sorter: BinaryHeap<PagesIndexChunk>,
    // pages chunk merge queue
    merge_queue: Vec<PagesIndexChunk>,
    // merge result
    result_buffer: VecDeque<MergedPagesChunk>,
    // result parquet file writer
    to_path: PathBuf,
    id_from: usize,
    // values/rows per row group
    row_group_values_limit: usize,
    // merge result array size
    array_page_size: usize,
    // null_pages_cache: HashMap<(DataType, usize), Rc<CompressedPage>>,
    // result page size
    data_page_size_limit_bytes: Option<usize>,
    max_part_file_size_bytes: Option<usize>,
}

#[derive(Debug, Clone)]
// decision maker what to do with page
pub enum MergeReorder {
    // pick page from stream, e.g. don't merge and write as is
    PickFromStream(usize, usize),
    // first vector - stream_id to pick from, second vector - streams which are merged
    Merge(Vec<usize>, Vec<usize>),
}

pub struct FileMergeOptions {
    index_cols: usize,
    data_page_size_limit: Option<usize>,
    row_group_values_limit: usize,
    array_page_size: usize,
}

#[derive(Debug, Clone)]
pub struct MergedFile {
    pub size_bytes: u64,
    pub values: usize,
    pub id: usize,
    pub min: Vec<ParquetValue>,
    pub max: Vec<ParquetValue>,
}

pub fn merge<R: Read + Seek>(
    mut readers: Vec<R>,
    to_path: PathBuf,
    out_part_id: usize,
    tbl_name: &str,
    level_id: usize,
    opts: Options,
) -> Result<Vec<MergedFile>> {
    // get arrow schemas of streams
    let arrow_schemas = readers
        .iter_mut()
        .map(|r| arrow2::io::parquet::read::read_metadata(r).and_then(|md| infer_schema(&md)))
        .collect::<arrow2::error::Result<Vec<Schema>>>()?;
    // make unified schema
    let arrow_schema = try_merge_arrow_schemas(arrow_schemas)?;
    // make parquet schema
    let parquet_schema = to_parquet_schema(&arrow_schema)?;
    // initialize parquet streams/readers
    let page_streams = readers
        .into_iter()
        // todo make dynamic page size
        .map(|r| CompressedPageIterator::try_new(r, opts.merge_max_page_size))
        .collect::<Result<Vec<_>>>()?;
    let streams_n = page_streams.len();

    let index_cols = (0..opts.index_cols)
        .map(|idx| parquet_schema.columns()[idx].to_owned())
        .collect::<Vec<_>>();

    let mut mr = Merger {
        index_cols,
        parquet_schema,
        arrow_schema,
        page_streams,
        tmp_arrays: (0..streams_n).map(|_| HashMap::new()).collect(),
        tmp_array_idx: (0..streams_n).map(|_| HashMap::new()).collect(),
        sorter: BinaryHeap::new(),
        merge_queue: Vec::with_capacity(100),
        result_buffer: VecDeque::with_capacity(10),
        to_path,
        id_from: out_part_id,
        row_group_values_limit: opts.row_group_values_limit,
        array_page_size: opts.array_page_size,
        // null_pages_cache: HashMap::new(),
        data_page_size_limit_bytes: opts.data_page_size_limit_bytes,
        max_part_file_size_bytes: opts.max_part_size_bytes,
    };

    Ok(mr.merge(tbl_name, level_id)?)
}

impl<R> Merger<R>
where R: Read + Seek
{
    // Create new merger

    // Get next chunk by stream_id. Chunk - all pages within row group
    fn next_stream_index_chunk(&mut self, stream_id: usize) -> Result<Option<PagesIndexChunk>> {
        let mut pages = Vec::with_capacity(self.index_cols.len());
        for col in &self.index_cols {
            let page = self.page_streams[stream_id].next_chunk(&col.path_in_schema)?;
            if page.is_none() {
                return Ok(None);
            }
            pages.push(page.unwrap());
        }

        Ok(Some(PagesIndexChunk::new(pages, stream_id)))
    }

    // Make null pages in case if there is no data pages for column
    fn make_null_pages(
        &mut self,
        cd: &ColumnDescriptor,
        field: Field,
        num_rows: usize,
        data_pagesize_limit: Option<usize>,
    ) -> Result<Vec<CompressedPage>> {
        let arr = new_null_array(field.data_type, num_rows);
        array_to_pages_simple(arr, cd.base_type.clone(), data_pagesize_limit)
    }

    // Main merge loop
    pub fn merge(&mut self, tbl_name: &str, level_id: usize) -> Result<Vec<MergedFile>> {
        // Init sorter with chunk per stream
        for stream_id in 0..self.page_streams.len() {
            if let Some(chunk) = self.next_stream_index_chunk(stream_id)? {
                // Push chunk to sorter
                self.sorter.push(chunk);
            }
        }
        // initialize parquet writer
        let write_opts = WriteOptions {
            write_statistics: true,
            version: Version::V2,
        };
        let mut merged_files: Vec<MergedFile> = Vec::new();
        'l1: for part_id in self.id_from.. {
            let w = File::create(&self.to_path.join(format!("{}.parquet", part_id)))?;
            let w = BufWriter::new(w);
            let mut seq_writer =
                FileSeqWriter::new(w, self.parquet_schema.clone(), write_opts, None);

            let mut min: Vec<ParquetValue> = Vec::new();
            let mut max: Vec<ParquetValue> = Vec::new();
            let mut num_values = 0;
            let mut index_pages = 0;
            let mut first = true;
            let mut some = false;
            // Request merge of index column
            'l2: while let Some(chunks) = self.next_index_chunk()? {
                some = true;
                // get descriptors of index/partition columns

                if first {
                    min = chunks[0].0.min_values.clone();
                }
                max = chunks.last().unwrap().0.max_values.clone();
                first = false;
                for col_id in 0..self.index_cols.len() {
                    for chunk in chunks.iter() {
                        if col_id == 0 {
                            num_values += chunk.num_values();
                            index_pages += 1;
                        }

                        for page in chunk.0.cols[col_id].iter() {
                            // write index pages
                            if let CompressedPage::Data(dp) = page {
                                histogram!("store.merger.uncompressed_index_page_size_bytes","table"=>tbl_name.to_string(),"level"=>level_id.to_string()).record(dp.uncompressed_size().to_f64().unwrap());
                                histogram!("store.merger.compressed_index_page_size_bytes","table"=>tbl_name.to_string(),"level"=>level_id.to_string()).record(dp.compressed_size().to_f64().unwrap());
                            }
                            seq_writer.write_page(page)?;
                        }
                    }
                    seq_writer.end_column()?;
                }

                // todo avoid cloning
                let cols = self
                    .parquet_schema
                    .columns()
                    .iter()
                    .skip(self.index_cols.len())
                    .map(|v| v.to_owned())
                    .collect::<Vec<_>>();

                let fields = self
                    .arrow_schema
                    .fields
                    .iter()
                    .skip(self.index_cols.len())
                    .map(|f| f.to_owned())
                    .collect::<Vec<_>>();

                // merge data pages based on reorder from MergedPagesChunk
                for (col, field) in cols.into_iter().zip(fields.into_iter()) {
                    for chunk in chunks.iter() {
                        let pages = match &chunk.1 {
                            // Exclusively pick page from the stream
                            MergeReorder::PickFromStream(stream_id, num_rows) => {
                                // If column exist for stream id then write it
                                if self.page_streams[*stream_id]
                                    .contains_column(&col.path_in_schema)
                                {
                                    self.page_streams[*stream_id]
                                        .next_chunk(&col.path_in_schema)?
                                        .unwrap()
                                } else {
                                    // for non-existant column make null page and write
                                    self.make_null_pages(
                                        &col,
                                        field.clone(),
                                        *num_rows,
                                        self.data_page_size_limit_bytes,
                                    )?
                                }
                            }
                            // Merge pages
                            MergeReorder::Merge(reorder, streams) => {
                                self.merge_data(&col, field.clone(), reorder, streams)?
                            }
                        };

                        // Write pages for column
                        for page in &pages {
                            if let CompressedPage::Data(dp) = page {
                                histogram!("store.merger.uncompressed_data_page_size_bytes","table"=>tbl_name.to_string(),"level"=>level_id.to_string()).record(dp.uncompressed_size().to_f64().unwrap());
                                histogram!("store.merger.compressed_data_page_size_bytes","table"=>tbl_name.to_string(),"level"=>level_id.to_string()).record(dp.compressed_size().to_f64().unwrap());
                            }
                            seq_writer.write_page(&page)?;
                        }
                    }

                    seq_writer.end_column()?;
                }
                histogram!("store.merger.row_group_index_pages","table"=>tbl_name.to_string(),"level"=>level_id.to_string()).record(index_pages.to_f64().unwrap());
                seq_writer.end_row_group()?;

                if let Some(max_part_file_size) = self.max_part_file_size_bytes {
                    let f = File::open(&self.to_path.join(format!("{}.parquet", part_id)))?;
                    if f.metadata().unwrap().size() > max_part_file_size as u64 {
                        // Add arrow schema to parquet metadata
                        let key_value_metadata = add_arrow_schema(&self.arrow_schema, None);
                        seq_writer.end(key_value_metadata)?;

                        let mf = MergedFile {
                            size_bytes: File::open(
                                &self.to_path.join(format!("{}.parquet", part_id)),
                            )?
                            .metadata()
                            .unwrap()
                            .size(),
                            values: num_values,
                            id: part_id,
                            min,
                            max,
                        };
                        merged_files.push(mf);
                        continue 'l1;
                    }
                }
            }

            if some {
                let key_value_metadata = add_arrow_schema(&self.arrow_schema, None);
                seq_writer.end(key_value_metadata)?;

                let mf = MergedFile {
                    size_bytes: File::open(&self.to_path.join(format!("{}.parquet", part_id)))?
                        .metadata()
                        .unwrap()
                        .size(),
                    values: num_values,
                    id: part_id,
                    min,
                    max,
                };
                merged_files.push(mf);
            } else {
                // delete empty file. We can do this safely because we write to temp file and then rename it
                fs::remove_file(&self.to_path.join(format!("{}.parquet", part_id)))?;
            }
            break;
        }
        Ok(merged_files)
    }

    // Merge data pages
    fn merge_data(
        &mut self,
        col: &ColumnDescriptor,
        field: Field,
        reorder: &[usize],
        streams: &[usize],
    ) -> Result<Vec<CompressedPage>> {
        // Call merger for type
        let out = match field.data_type().to_physical_type() {
            ArrowPhysicalType::Boolean => merge_arrays!(
                self,
                field,
                TmpArray::Boolean,
                BooleanArray,
                MutableBooleanArray,
                col,
                reorder,
                streams
            ),
            ArrowPhysicalType::Primitive(pt) => match pt {
                ArrowPrimitiveType::Int8 => merge_primitive_arrays!(
                    self,
                    field,
                    TmpArray::Int8,
                    Int8Array,
                    MutablePrimitiveArray<i8>,
                    col,
                    reorder,
                    streams
                ),
                ArrowPrimitiveType::Int16 => merge_primitive_arrays!(
                    self,
                    field,
                    TmpArray::Int16,
                    Int16Array,
                    MutablePrimitiveArray<i16>,
                    col,
                    reorder,
                    streams
                ),
                ArrowPrimitiveType::Int32 => merge_primitive_arrays!(
                    self,
                    field,
                    TmpArray::Int32,
                    Int32Array,
                    MutablePrimitiveArray<i32>,
                    col,
                    reorder,
                    streams
                ),
                ArrowPrimitiveType::Int64 => merge_primitive_arrays!(
                    self,
                    field,
                    TmpArray::Int64,
                    Int64Array,
                    MutablePrimitiveArray<i64>,
                    col,
                    reorder,
                    streams
                ),
                ArrowPrimitiveType::Int128 => merge_primitive_arrays!(
                    self,
                    field,
                    TmpArray::Int128,
                    Int128Array,
                    MutablePrimitiveArray<i128>,
                    col,
                    reorder,
                    streams
                ),
                ArrowPrimitiveType::UInt8 => merge_primitive_arrays!(
                    self,
                    field,
                    TmpArray::UInt8,
                    UInt8Array,
                    MutablePrimitiveArray<u8>,
                    col,
                    reorder,
                    streams
                ),
                ArrowPrimitiveType::UInt16 => merge_primitive_arrays!(
                    self,
                    field,
                    TmpArray::UInt16,
                    UInt16Array,
                    MutablePrimitiveArray<u16>,
                    col,
                    reorder,
                    streams
                ),
                ArrowPrimitiveType::UInt32 => merge_primitive_arrays!(
                    self,
                    field,
                    TmpArray::UInt32,
                    UInt32Array,
                    MutablePrimitiveArray<u32>,
                    col,
                    reorder,
                    streams
                ),
                ArrowPrimitiveType::UInt64 => merge_primitive_arrays!(
                    self,
                    field,
                    TmpArray::UInt64,
                    UInt64Array,
                    MutablePrimitiveArray<u64>,
                    col,
                    reorder,
                    streams
                ),
                ArrowPrimitiveType::Float32 => merge_primitive_arrays!(
                    self,
                    field,
                    TmpArray::Float32,
                    Float32Array,
                    MutablePrimitiveArray<f32>,
                    col,
                    reorder,
                    streams
                ),
                ArrowPrimitiveType::Float64 => merge_primitive_arrays!(
                    self,
                    field,
                    TmpArray::Float64,
                    Float64Array,
                    MutablePrimitiveArray<f64>,
                    col,
                    reorder,
                    streams
                ),
                _ => unreachable!(
                    "Unsupported physical type {:?}",
                    field.data_type().to_physical_type()
                ),
            },
            ArrowPhysicalType::Binary => merge_arrays!(
                self,
                field,
                TmpArray::Binary,
                BinaryArray<i32>,
                MutableBinaryArray<i32>,
                col,
                reorder,
                streams
            ),
            ArrowPhysicalType::LargeBinary => merge_arrays!(
                self,
                field,
                TmpArray::LargeBinary,
                BinaryArray<i64>,
                MutableBinaryArray<i64>,
                col,
                reorder,
                streams
            ),
            ArrowPhysicalType::Utf8 => merge_arrays!(
                self,
                field,
                TmpArray::Utf8,
                Utf8Array<i32>,
                MutableUtf8Array<i32>,
                col,
                reorder,
                streams
            ),
            ArrowPhysicalType::LargeUtf8 => merge_arrays!(
                self,
                field,
                TmpArray::LargeUtf8,
                Utf8Array<i64>,
                MutableUtf8Array<i64>,
                col,
                reorder,
                streams
            ),
            ArrowPhysicalType::List => {
                if let DataType::List(inner) = field.data_type() {
                    let inner_field = *inner.to_owned();
                    match inner.data_type.to_physical_type() {
                        ArrowPhysicalType::Boolean => {
                            merge_list_arrays!(self, field, i32, TmpArray::ListBoolean,BooleanArray,MutableListArray<i32,MutableBooleanArray>,col,reorder,streams)
                        }
                        ArrowPhysicalType::Primitive(pt) => match pt {
                            ArrowPrimitiveType::Int8 => {
                                merge_list_primitive_arrays!(self, field, inner_field,i32, TmpArray::ListInt8,Int8Array,MutableListArray<i32,MutablePrimitiveArray<i8>>,col,reorder,streams)
                            }
                            ArrowPrimitiveType::Int16 => {
                                merge_list_primitive_arrays!(self, field, inner_field,i32, TmpArray::ListInt16,Int16Array,MutableListArray<i32,MutablePrimitiveArray<i16>>,col,reorder,streams)
                            }
                            ArrowPrimitiveType::Int32 => {
                                merge_list_primitive_arrays!(self, field, inner_field,i32, TmpArray::ListInt32,Int32Array,MutableListArray<i32,MutablePrimitiveArray<i32>>,col,reorder,streams)
                            }
                            ArrowPrimitiveType::Int64 => {
                                merge_list_primitive_arrays!(self, field, inner_field,i32, TmpArray::ListInt64,Int64Array,MutableListArray<i32,MutablePrimitiveArray<i64>>,col,reorder,streams)
                            }
                            ArrowPrimitiveType::Int128 => {
                                merge_list_primitive_arrays!(self, field, inner_field,i32, TmpArray::ListInt128,Int128Array,MutableListArray<i32,MutablePrimitiveArray<i128>>,col,reorder,streams)
                            }
                            ArrowPrimitiveType::UInt8 => {
                                merge_list_primitive_arrays!(self, field, inner_field,i32, TmpArray::ListUInt8,UInt8Array,MutableListArray<i32,MutablePrimitiveArray<u8>>,col,reorder,streams)
                            }
                            ArrowPrimitiveType::UInt16 => {
                                merge_list_primitive_arrays!(self, field, inner_field,i32, TmpArray::ListUInt16,UInt16Array,MutableListArray<i32,MutablePrimitiveArray<u16>>,col,reorder,streams)
                            }
                            ArrowPrimitiveType::UInt32 => {
                                merge_list_primitive_arrays!(self, field, inner_field,i32, TmpArray::ListUInt32,UInt32Array,MutableListArray<i32,MutablePrimitiveArray<u32>>,col,reorder,streams)
                            }
                            ArrowPrimitiveType::UInt64 => {
                                merge_list_primitive_arrays!(self, field, inner_field,i32, TmpArray::ListUInt64,UInt64Array,MutableListArray<i32,MutablePrimitiveArray<u64>>,col,reorder,streams)
                            }
                            ArrowPrimitiveType::Float32 => {
                                merge_list_primitive_arrays!(self, field, inner_field,i32, TmpArray::ListFloat32,Float32Array,MutableListArray<i32,MutablePrimitiveArray<f32>>,col,reorder,streams)
                            }
                            ArrowPrimitiveType::Float64 => {
                                merge_list_primitive_arrays!(self, field, inner_field,i32, TmpArray::ListFloat64,Float64Array,MutableListArray<i32,MutablePrimitiveArray<f64>>,col,reorder,streams)
                            }
                            _ => unreachable!("list primitive type {pt:?} is not supported"),
                        },
                        ArrowPhysicalType::Binary => {
                            merge_list_arrays!(self, field, i32, TmpArray::ListBinary,BinaryArray<i32>,MutableListArray<i32,MutableBinaryArray<i32>>,col,reorder,streams)
                        }
                        ArrowPhysicalType::LargeBinary => {
                            merge_list_arrays!(self, field, i32, TmpArray::ListLargeBinary,BinaryArray<i64>,MutableListArray<i32,MutableBinaryArray<i64>>,col,reorder,streams)
                        }
                        ArrowPhysicalType::Utf8 => {
                            merge_list_arrays!(self, field, i32, TmpArray::ListUtf8,Utf8Array<i32>,MutableListArray<i32,MutableUtf8Array<i32>>,col,reorder,streams)
                        }
                        ArrowPhysicalType::LargeUtf8 => {
                            merge_list_arrays!(self, field, i32, TmpArray::ListLargeUtf8,Utf8Array<i64>,MutableListArray<i32,MutableUtf8Array<i64>>,col,reorder,streams)
                        }
                        _ => unreachable!(
                            "list type {:?} is not supported",
                            inner.data_type.to_physical_type()
                        ),
                    }
                } else {
                    unreachable!("list field {:?} type is not supported", field.data_type())
                }
            }
            _ => unreachable!(
                "unsupported physical type {:?}",
                field.data_type().to_physical_type()
            ),
        };

        let pages =
            array_to_pages_simple(out, col.base_type.clone(), self.data_page_size_limit_bytes)?;

        Ok(pages)
    }

    // Queue merger
    fn merge_queue(&mut self) -> Result<()> {
        let mut buf = vec![];
        // read page chunks from queue and convert to arrow chunks
        let arrow_chunks = self
            .merge_queue
            .drain(..)
            .map(|chunk| chunk.to_arrow_chunk(&mut buf, &self.index_cols))
            .collect::<Result<Vec<_>>>()?;

        // get streams from chunks
        let mut streams = arrow_chunks
            .iter()
            .map(|chunk| chunk.stream)
            .collect::<Vec<_>>();
        // remove duplicates in case if there are several chunks from the same stream
        streams.dedup();
        // merge
        let merged_chunks = merge_chunks(arrow_chunks, self.array_page_size)?;

        // convert arrow to parquet page chunks
        for chunk in merged_chunks {
            let pages_chunk = PagesIndexChunk::from_arrow(chunk.cols.as_slice(), &self.index_cols)?;
            let merged_chunk = MergedPagesChunk::new(
                pages_chunk,
                MergeReorder::Merge(chunk.reorder, streams.clone()),
            );
            // push to the end of result buffer
            self.result_buffer.push_back(merged_chunk);
        }

        Ok(())
    }

    // Get the next index chunk
    fn next_index_chunk(&mut self) -> Result<Option<Vec<MergedPagesChunk>>> {
        // iterate over chunks in heap
        while let Some(chunk) = self.sorter.pop() {
            // try to get next chunk of this stream
            if let Some(next) = self.next_stream_index_chunk(chunk.stream)? {
                // push next chunk to sorter
                self.sorter.push(next);
            }

            // push chunk to merge queue
            self.merge_queue.push(chunk);
            // check intersection of first chunk with merge queue
            // all the intersected chunks should be merged
            while check_intersection(&self.merge_queue, self.sorter.peek()) {
                // in case of intersection, take chunk and add it to merge queue
                let next = self.sorter.pop().unwrap();
                // try to take next chunk of stream and add it to sorter
                if let Some(chunk) = self.next_stream_index_chunk(next.stream)? {
                    self.sorter.push(chunk);
                }
                // push chunk to merge queue
                self.merge_queue.push(next);
            }

            // check queue len. Queue len may be 1 if there is no intersection
            if self.merge_queue.len() > 1 {
                // in case of intersection, merge queue
                self.merge_queue()?;
            } else {
                // queue contains only one chunk, so we can just push it to result
                let chunk = self.merge_queue.pop().unwrap();
                let num_vals = chunk.len();
                let chunk_stream = chunk.stream;
                self.result_buffer.push_back(MergedPagesChunk::new(
                    chunk,
                    MergeReorder::PickFromStream(chunk_stream, num_vals),
                ));
            }

            // try drain result
            if let Some(res) = self.try_drain_result(self.row_group_values_limit) {
                return Ok(Some(res));
            }
        }

        Ok(self.try_drain_result(1))
    }

    // check result length and drain if needed
    fn try_drain_result(&mut self, values_limit: usize) -> Option<Vec<MergedPagesChunk>> {
        if self.result_buffer.is_empty() {
            return None;
        }
        let mut res = vec![];
        let first = self.result_buffer.pop_front().unwrap();
        let mut values_limit = values_limit as i64;
        values_limit -= first.num_values() as i64;
        res.push(first);
        while values_limit > 0 && !self.result_buffer.is_empty() {
            let next_values = self.result_buffer.front().unwrap().num_values() as i64;
            if values_limit - next_values < 0 {
                break;
            }

            values_limit -= next_values;
            let chunk = self.result_buffer.pop_front().unwrap();
            res.push(chunk)
        }

        Some(res)
    }
}
