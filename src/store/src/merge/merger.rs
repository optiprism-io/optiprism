use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::io::Read;
use std::io::Seek;
use std::io::Write;

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
use arrow2::array::TryPush;
use arrow2::array::UInt16Array;
use arrow2::array::UInt32Array;
use arrow2::array::UInt64Array;
use arrow2::array::UInt8Array;
use arrow2::array::Utf8Array;
use arrow2::bitmap::Bitmap;
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
use crate::merge::arrow::merge_chunks;
use crate::merge::arrow::try_merge_schemas as try_merge_arrow_schemas;
use crate::merge::parquet::array_to_pages_simple;
use crate::merge::parquet::check_intersection;
use crate::merge::parquet::data_page_to_array;
use crate::merge::parquet::ColumnPath;
use crate::merge::parquet::CompressedPageIterator;
use crate::merge::parquet::MergedPagesChunk;
use crate::merge::parquet::PagesChunk;

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

// Merge multiple arrays based on reorder
macro_rules! merge_arrays_inner {
    ($self:expr,$field:expr,$tmp_ty:path,$in_ty:ty, $out_ty:ty,$col:expr,$reorder:expr,$streams:expr) => {{
        let col_path: ColumnPath = $col.path_in_schema.clone();
        let mut buf = vec![];
        // check if column is exist in each stream
        let col_exist_per_stream = $self
            .page_streams
            .iter()
            .enumerate()
            .map(|(stream_id, stream)| {
                $streams.contains(&stream_id) && stream.contains_column(&col_path)
            })
            .collect::<Vec<bool>>();

        // pop arrays from tmp_arrays
        // used to downcast (TmpArray) and avoid borrow checker
        let mut arrs = $self
            .tmp_arrays
            .iter_mut()
            .enumerate()
            .map(|(stream_id, cols)| {
                if $streams.contains(&stream_id) {
                    match cols.remove(&col_path) {
                        None => None,
                        Some(v) => {
                            if let $tmp_ty(arr) = v {
                                Some(arr)
                            } else {
                                None
                            }
                        }
                    }
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        // calculate array indexes
        let mut arrs_idx: Vec<usize> = $self
            .tmp_array_idx
            .iter()
            .map(|cols| match cols.get(&col_path) {
                Some(idx) => *idx,
                None => 0,
            })
            .collect();

        // make output array
        // perf: make reusable buffer
        let mut out = <$out_ty>::with_capacity($reorder.len());
        // go through each row idx in reorder, get stream id for row
        for idx in 0..$reorder.len() {
            let stream_id = $reorder[idx];
            // push null if column doesn't exist
            if !col_exist_per_stream[stream_id] {
                out.push_null();
                continue;
            }

            // downcast array if it is not exist in tmp
            if arrs[stream_id].is_none() {
                let page = $self.page_streams[stream_id].next_page(&col_path)?.unwrap();
                if let CompressedPage::Data(page) = page {
                    let any_arr = data_page_to_array(page, &$col, $field.clone(), &mut buf)?;
                    let tmp_arr = any_arr.as_any().downcast_ref::<$in_ty>().unwrap().clone();
                    arrs[stream_id] = Some(tmp_arr);
                    arrs_idx[stream_id] = 0;
                }
            }

            let cur_idx = arrs_idx[stream_id];
            let arr = arrs[stream_id].as_ref().unwrap();
            if arr.is_null(cur_idx) {
                out.push_null();
            } else {
                out.push(Some(arr.value(cur_idx)));
            }

            if cur_idx == arr.len() - 1 {
                arrs[stream_id] = None;
                arrs_idx[stream_id] = 0;
            } else {
                arrs_idx[stream_id] += 1;
            }
        }

        // move all arrays back to tmp
        for (stream_id, maybe_arr) in arrs.into_iter().enumerate() {
            if let Some(arr) = maybe_arr {
                $self.tmp_arrays[stream_id].insert(col_path.clone(), $tmp_ty(arr));
            }
        }

        for (stream_id, idx) in arrs_idx.into_iter().enumerate() {
            $self.tmp_array_idx[stream_id].insert(col_path.clone(), idx);
        }

        out
    }};
}

// merge data arrays
macro_rules! merge_arrays {
    ($self:expr,$field:expr,$tmp_ty:path,$in_ty:ty, $out_ty:ty,$col:expr,$reorder:expr,$streams:expr) => {{
        let mut out = merge_arrays_inner!(
            $self, $field, $tmp_ty, $in_ty, $out_ty, $col, $reorder, $streams
        );

        out.as_box()
    }};
}

// merge primitive arrays
macro_rules! merge_primitive_arrays {
    ($self:expr,$field:expr,$tmp_ty:path,$in_ty:ty, $out_ty:ty,$col:expr,$reorder:expr,$streams:expr) => {{
        let out = merge_arrays_inner!(
            $self, $field, $tmp_ty, $in_ty, $out_ty, $col, $reorder, $streams
        );

        out.to($field.data_type().to_owned()).as_box()
    }};
}

// merge list arrays
macro_rules! merge_list_arrays_inner {
    ($self:expr,$field:expr,$offset:ty, $tmp_ty:path,$in_ty:ty, $out_ty:ty,$col:expr,$reorder:expr,$streams:expr) => {{
        let col_path: ColumnPath = $col.path_in_schema.clone();
        let mut buf = vec![];
        let col_exist_per_stream = $self
            .page_streams
            .iter()
            .enumerate()
            .map(|(stream_id, stream)| {
                $streams.contains(&stream_id) && stream.contains_column(&col_path)
            })
            .collect::<Vec<bool>>();

        let mut arrs = $self
            .tmp_arrays
            .iter_mut()
            .enumerate()
            .map(|(stream_id, cols)| {
                if $streams.contains(&stream_id) {
                    match cols.remove(&col_path) {
                        None => None,
                        Some(v) => {
                            if let $tmp_ty(arr, offsets, validity, num_vals) = v {
                                Some((arr, offsets, validity, num_vals))
                            } else {
                                None
                            }
                        }
                    }
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        let mut arrs_idx: Vec<usize> = $self
            .tmp_array_idx
            .iter()
            .map(|cols| match cols.get(&col_path) {
                Some(idx) => *idx,
                None => 0,
            })
            .collect();

        let mut out = <$out_ty>::with_capacity($reorder.len());
        for idx in 0..$reorder.len() {
            let stream_id = $reorder[idx];
            if !col_exist_per_stream[stream_id] {
                out.push_null();
                continue;
            }

            if arrs[stream_id].is_none() {
                let page = $self.page_streams[stream_id].next_page(&col_path)?.unwrap();
                if let CompressedPage::Data(page) = page {
                    let any_arr = data_page_to_array(page, &$col, $field.clone(), &mut buf)?;
                    let list_arr = any_arr
                        .as_any()
                        .downcast_ref::<ListArray<$offset>>()
                        .unwrap()
                        .clone();
                    let arr = list_arr
                        .values()
                        .as_any()
                        .downcast_ref::<$in_ty>()
                        .unwrap()
                        .clone();
                    let offsets = list_arr.offsets().clone();
                    let validity = list_arr.validity().map(|v| v.clone());
                    arrs[stream_id] = Some((arr, offsets, validity, list_arr.len()));
                    arrs_idx[stream_id] = 0;
                }
            }

            let cur_idx = arrs_idx[stream_id];
            let (arr, offsets, validity, num_vals) = arrs[stream_id].as_ref().unwrap();
            if validity
                .as_ref()
                .map(|x| !x.get_bit(cur_idx))
                .unwrap_or(false)
            {
                out.push_null();
            } else {
                let (start, end) = offsets.start_end(cur_idx);
                let length = end - start;
                // FIXME avoid clone?
                let vals = arr.clone().sliced(start, length);
                out.try_push(Some(vals.into_iter()))?;
            }

            if cur_idx == *num_vals - 1 {
                arrs[stream_id] = None;
                arrs_idx[stream_id] = 0;
            } else {
                arrs_idx[stream_id] += 1;
            }
        }

        for (stream_id, maybe_arr) in arrs.into_iter().enumerate() {
            if let Some((a, o, v, l)) = maybe_arr {
                $self.tmp_arrays[stream_id].insert(col_path.clone(), $tmp_ty(a, o, v, l));
            }
        }

        for (stream_id, idx) in arrs_idx.into_iter().enumerate() {
            $self.tmp_array_idx[stream_id].insert(col_path.clone(), idx);
        }

        out
    }};
}

macro_rules! merge_list_arrays {
    ($self:expr,$field:expr,$offset:ty, $tmp_ty:path,$in_ty:ty, $out_ty:ty,$col:expr,$reorder:expr,$streams:expr) => {{
        let mut out = merge_list_arrays_inner!(
            $self, $field, $offset, $tmp_ty, $in_ty, $out_ty, $col, $reorder, $streams
        );

        out.as_box()
    }};
}

macro_rules! merge_list_primitive_arrays {
    ($self:expr,$field:expr,$inner_field:expr,$offset:ty, $tmp_ty:path,$in_ty:ty, $out_ty:ty,$col:expr,$reorder:expr,$streams:expr) => {{
        let col_path: ColumnPath = $col.path_in_schema.clone();
        let mut buf = vec![];
        let col_exist_per_stream = $self
            .page_streams
            .iter()
            .enumerate()
            .map(|(stream_id, stream)| {
                $streams.contains(&stream_id) && stream.contains_column(&col_path)
            })
            .collect::<Vec<bool>>();

        let mut arrs = $self
            .tmp_arrays
            .iter_mut()
            .enumerate()
            .map(|(stream_id, cols)| {
                if $streams.contains(&stream_id) {
                    match cols.remove(&col_path) {
                        None => None,
                        Some(v) => {
                            if let $tmp_ty(arr, offsets, validity, num_vals) = v {
                                Some((arr, offsets, validity, num_vals))
                            } else {
                                None
                            }
                        }
                    }
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        let mut arrs_idx: Vec<usize> = $self
            .tmp_array_idx
            .iter()
            .map(|cols| match cols.get(&col_path) {
                Some(idx) => *idx,
                None => 0,
            })
            .collect();

        let mut out = <$out_ty>::with_capacity($reorder.len());
        for idx in 0..$reorder.len() {
            let stream_id = $reorder[idx];
            if !col_exist_per_stream[stream_id] {
                out.push_null();
                continue;
            }

            if arrs[stream_id].is_none() {
                let page = $self.page_streams[stream_id].next_page(&col_path)?.unwrap();
                if let CompressedPage::Data(page) = page {
                    let any_arr = data_page_to_array(page, &$col, $field.clone(), &mut buf)?;
                    let list_arr = any_arr
                        .as_any()
                        .downcast_ref::<ListArray<$offset>>()
                        .unwrap()
                        .clone();
                    let arr = list_arr
                        .values()
                        .as_any()
                        .downcast_ref::<$in_ty>()
                        .unwrap()
                        .clone()
                        .to($inner_field.data_type().clone());
                    let offsets = list_arr.offsets().clone();
                    let validity = list_arr.validity().map(|v| v.clone());
                    arrs[stream_id] = Some((arr, offsets, validity, list_arr.len()));
                    arrs_idx[stream_id] = 0;
                }
            }

            let cur_idx = arrs_idx[stream_id];
            let (arr, offsets, validity, num_vals) = arrs[stream_id].as_ref().unwrap();
            if validity
                .as_ref()
                .map(|x| !x.get_bit(cur_idx))
                .unwrap_or(false)
            {
                out.push_null();
            } else {
                let (start, end) = offsets.start_end(cur_idx);
                let length = end - start;
                // FIXME avoid clone?
                let vals = arr.clone().sliced(start, length);
                out.try_push(Some(vals.into_iter()))?;
            }

            if cur_idx == *num_vals - 1 {
                arrs[stream_id] = None;
                arrs_idx[stream_id] = 0;
            } else {
                arrs_idx[stream_id] += 1;
            }
        }

        for (stream_id, maybe_arr) in arrs.into_iter().enumerate() {
            if let Some((a, o, v, l)) = maybe_arr {
                $self.tmp_arrays[stream_id].insert(col_path.clone(), $tmp_ty(a, o, v, l));
            }
        }

        for (stream_id, idx) in arrs_idx.into_iter().enumerate() {
            $self.tmp_array_idx[stream_id].insert(col_path.clone(), idx);
        }

        out.as_box()
    }};
}

pub struct Merger<R, W>
where
    R: Read,
    W: Write,
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
    sorter: BinaryHeap<PagesChunk>,
    // pages chunk merge queue
    merge_queue: Vec<PagesChunk>,
    // merge result
    result_buffer: VecDeque<MergedPagesChunk>,
    // result parquet file writer
    writer: FileSeqWriter<W>,
    // values/rows per row group
    row_group_values_limit: usize,
    // merge result array size
    array_page_size: usize,
    // null_pages_cache: HashMap<(DataType, usize), Rc<CompressedPage>>,
    // result page size
    data_page_size_limit: Option<usize>,
}

#[derive(Debug, Clone)]
// decision maker what to do with page
pub enum MergeReorder {
    // pick page from stream, e.g. don't merge and write as is
    PickFromStream(usize, usize),
    // first vector - stream_id to pick from, second vector - streams which are merged
    Merge(Vec<usize>, Vec<usize>),
}

impl<R, W> Merger<R, W>
where
    R: Read + Seek,
    W: Write,
{
    // Create new merger

    pub fn try_new(
        mut readers: Vec<R>,
        writer: W,
        index_cols: usize,
        data_page_size_limit: Option<usize>,
        row_group_values_limit: usize,
        array_page_size: usize,
    ) -> Result<Self> {
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
            .map(|r| CompressedPageIterator::try_new(r))
            .collect::<Result<Vec<_>>>()?;
        let streams_n = page_streams.len();

        // initialize parquet writer
        let opts = WriteOptions {
            write_statistics: true,
            version: Version::V2,
        };
        let seq_writer = FileSeqWriter::new(writer, parquet_schema.clone(), opts, None);
        // get descriptors of index/partition columns
        let index_cols = (0..index_cols)
            .map(|idx| parquet_schema.columns()[idx].to_owned())
            .collect::<Vec<_>>();

        Ok(Self {
            index_cols,
            parquet_schema,
            arrow_schema,
            page_streams,
            tmp_arrays: (0..streams_n).map(|_| HashMap::new()).collect(),
            tmp_array_idx: (0..streams_n).map(|_| HashMap::new()).collect(),
            sorter: BinaryHeap::new(),
            merge_queue: Vec::with_capacity(100),
            result_buffer: VecDeque::with_capacity(10),
            writer: seq_writer,
            row_group_values_limit,
            array_page_size,
            // null_pages_cache: HashMap::new(),
            data_page_size_limit,
        })
    }

    // Get next chunk by stream_id. Chunk - all pages within row group
    fn next_stream_index_chunk(&mut self, stream_id: usize) -> Result<Option<PagesChunk>> {
        let mut pages = Vec::with_capacity(self.index_cols.len());
        for col in &self.index_cols {
            let page = self.page_streams[stream_id].next_chunk(&col.path_in_schema)?;
            if page.is_none() {
                return Ok(None);
            }
            pages.push(page.unwrap());
        }

        Ok(Some(PagesChunk::new(pages, stream_id)))
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
    pub fn merge(&mut self) -> Result<()> {
        // Init sorter with chunk per stream
        for stream_id in 0..self.page_streams.len() {
            if let Some(chunk) = self.next_stream_index_chunk(stream_id)? {
                // Push chunk to sorter
                self.sorter.push(chunk);
            }
        }

        // Request merge of index column
        while let Some(chunks) = self.next_index_chunk()? {
            for col_id in 0..self.index_cols.len() {
                for chunk in chunks.iter() {
                    for page in chunk.0.cols[col_id].iter() {
                        // write index pages
                        self.writer.write_page(page)?;
                    }
                }
                self.writer.end_column()?;
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
                            if self.page_streams[*stream_id].contains_column(&col.path_in_schema) {
                                self.page_streams[*stream_id]
                                    .next_chunk(&col.path_in_schema)?
                                    .unwrap()
                            } else {
                                // for non-existant column make null page and write
                                self.make_null_pages(
                                    &col,
                                    field.clone(),
                                    *num_rows,
                                    self.data_page_size_limit,
                                )?
                            }
                        }
                        // Merge pages
                        MergeReorder::Merge(reorder, streams) => {
                            self.merge_data(&col, field.clone(), reorder, streams)?
                        }
                    };

                    // Write pages for column
                    for page in pages {
                        self.writer.write_page(&page)?;
                    }
                }

                self.writer.end_column()?;
            }
            self.writer.end_row_group()?;
        }

        // Add arrow schema to parquet metadata
        let key_value_metadata = add_arrow_schema(&self.arrow_schema, None);
        self.writer.end(key_value_metadata)?;

        Ok(())
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

        let pages = array_to_pages_simple(out, col.base_type.clone(), self.data_page_size_limit)?;

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
            let pages_chunk = PagesChunk::from_arrow(chunk.arrs.as_slice(), &self.index_cols)?;
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
                let num_vals = chunk.num_values();
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
