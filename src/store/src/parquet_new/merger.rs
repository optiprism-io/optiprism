use std::any::Any;
use std::{cmp, io};
use std::collections::{BinaryHeap, VecDeque};
use std::collections::HashMap;
use std::io::Read;
use std::io::Seek;
use std::io::Write;
use std::marker::PhantomData;
use std::ops::{Range, SubAssign};
use std::ops::RangeBounds;
use std::rc::Rc;

use arrow2::array::{Array, ListArray, MutableArray, MutableBinaryArray, MutableListArray, MutablePrimitiveArray, new_null_array, PrimitiveArray, TryExtend, TryPush, Utf8Array};
use arrow2::array::BinaryArray;
use arrow2::array::BooleanArray;
use arrow2::array::FixedSizeBinaryArray;
use arrow2::array::Float32Array;
use arrow2::array::Float64Array;
use arrow2::array::Int32Array;
use arrow2::array::Int64Array;
use arrow2::bitmap::Bitmap;
use arrow2::datatypes::PhysicalType as ArrowPhysicalType;
use arrow2::datatypes::{DataType, Field};
use arrow2::ffi::mmap::slice;
use arrow2::io::parquet::read::column_iter_to_arrays;
use arrow2::io::parquet::read::schema::parquet_to_arrow_schema;
use arrow2::io::parquet::write::array_to_columns;
use arrow2::offset::OffsetsBuffer;
use parquet2::encoding::Encoding;
use parquet2::metadata::ColumnDescriptor;
use parquet2::metadata::SchemaDescriptor;
use parquet2::page::CompressedDataPage;
use parquet2::page::CompressedPage;
use parquet2::page::Page;
use parquet2::read::decompress;
use parquet2::schema::Repetition;
use parquet2::schema::types::{FieldInfo, GroupLogicalType};
use parquet2::schema::types::ParquetType;
use parquet2::schema::types::PhysicalType;
use parquet2::schema::types::PrimitiveType;
use parquet2::write::FileSeqWriter;
use parquet2::write::Version;
use parquet2::write::WriteOptions;

use crate::error::Result;
use crate::error::StoreError;
use crate::parquet::parquet::array_to_page;
use crate::parquet_new::arrow::merge;
use crate::parquet_new::arrow::ArrowChunk;
use crate::parquet_new::parquet::{array_to_pages_simple, ColumnPath, MergedPagesChunk, pages_to_arrays, PagesChunk};
use crate::parquet_new::parquet::check_intersection;
use crate::parquet_new::parquet::data_page_to_array;
use crate::parquet_new::parquet::CompressedPageIterator;
use crate::parquet_new::schema::try_merge_schemas;


enum TmpListArray {
    Int64Array(Int64Array),
    BooleanArray(BooleanArray),
    FixedSizeBinaryArray(FixedSizeBinaryArray),
    BinaryArray(BinaryArray<i32>),
    Utf8Array(Utf8Array<i32>),
}

// this is a temporary array used to merge data pages avoiding downcasting
enum TmpArray {
    Int64(Int64Array),
    Boolean(BooleanArray),
    FixedSizeBinary(FixedSizeBinaryArray),
    Binary(BinaryArray<i32>),
    Utf8(Utf8Array<i32>),
    ListInt64(Int64Array, OffsetsBuffer<i32>, Option<Bitmap>, usize),
    ListBoolean(BooleanArray, OffsetsBuffer<i32>, Option<Bitmap>, usize),
    ListFixedSizeBinary(FixedSizeBinaryArray, OffsetsBuffer<i32>, Option<Bitmap>, usize),
    ListBinary(BinaryArray<i32>, OffsetsBuffer<i32>, Option<Bitmap>, usize),
    ListUtf8(Utf8Array<i32>, OffsetsBuffer<i32>, Option<Bitmap>, usize),
}


fn column_to_field(cd: &ColumnDescriptor) -> Field {
    parquet_to_arrow_schema(vec![cd.base_type.clone()].as_slice()).pop().unwrap()
}

macro_rules! pop_tmp_array {
    ($self:expr, $ty:ty) => {{

    }}
}


macro_rules! merge_arrays {
    ($self:expr,$tmp_ty:path,$in_ty:ty, $out_ty:ty,$col:expr,$reorder:expr,$streams:expr)=> {{
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
                println!("page {:#?}", page);
                if let CompressedPage::Data(page) = page {
                    let any_arr = data_page_to_array(page, &$col, &mut buf)?;
                    println!("any arr {:#?}", any_arr.data_type());
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

        for (stream_id, maybe_arr) in arrs.into_iter().enumerate() {
            if let Some(arr) = maybe_arr {
                $self.tmp_arrays[stream_id].insert(col_path.clone(), $tmp_ty(arr));
            }
        }

        for (stream_id, idx) in arrs_idx.into_iter().enumerate() {
            $self.tmp_array_idx[stream_id].insert(col_path.clone(), idx);
        }

        out.as_box()
    }}
}

macro_rules! merge_list_arrays {
    ($self:expr,$offset:ty, $tmp_ty:path,$in_ty:ty, $out_ty:ty,$col:expr,$reorder:expr,$streams:expr)=> {{
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
                                Some((arr, offsets, validity,num_vals))
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
                    let any_arr = data_page_to_array(page, &$col, &mut buf)?;
                    println!("any arr {:#?}", any_arr.data_type());
                    let list_arr = any_arr.as_any().downcast_ref::<ListArray<$offset>>().unwrap().clone();
                    let arr = list_arr.values().as_any().downcast_ref::<$in_ty>().unwrap().clone();
                    let offsets = list_arr.offsets().clone();
                    let validity = list_arr.validity().map(|v| v.clone());
                    arrs[stream_id] = Some((arr, offsets, validity, list_arr.len()));
                    arrs_idx[stream_id] = 0;
                }
            }

            let cur_idx = arrs_idx[stream_id];
            let (arr, offsets, validity, num_vals) = arrs[stream_id].as_ref().unwrap();
            if validity.as_ref().map(|x| !x.get_bit(cur_idx)).unwrap_or(false) {
                out.push_null();
            } else {
                println!("stream id {}",stream_id);
                println!("cur idx {}", cur_idx);
                println!("arr {:?} {}", arr,arr.len());
                println!("offsets len {}", offsets.len());
                println!("{:?}", arrs[stream_id]);
                let (start, end) = offsets.start_end(cur_idx);
                let length = end - start;
                // TODO avoid clone?
                let vals = arr.clone().sliced(start, length);
                out.try_push(Some(vals.into_iter()))?;
            }

            if cur_idx == *num_vals-1 {
                arrs[stream_id] = None;
                arrs_idx[stream_id] = 0;
            } else {
                arrs_idx[stream_id] += 1;
            }
        }

        for (stream_id, maybe_arr) in arrs.into_iter().enumerate() {
            if let Some((a, o, v,l)) = maybe_arr {
                $self.tmp_arrays[stream_id].insert(col_path.clone(), $tmp_ty(a, o, v,l));
            }
        }

        for (stream_id, idx) in arrs_idx.into_iter().enumerate() {
            $self.tmp_array_idx[stream_id].insert(col_path.clone(), idx);
        }

        out.as_box()
    }}
}

pub struct FileMerger<R, W>
    where
        R: Read,
        W: Write,
{
    index_cols: Vec<ColumnDescriptor>,
    schema: SchemaDescriptor,
    page_streams: Vec<CompressedPageIterator<R>>,
    tmp_arrays: Vec<HashMap<ColumnPath, TmpArray>>,
    tmp_array_idx: Vec<HashMap<ColumnPath, usize>>,
    sorter: BinaryHeap<PagesChunk>,
    merge_queue: Vec<PagesChunk>,
    result_buffer: VecDeque<MergedPagesChunk>,
    writer: FileSeqWriter<W>,
    row_group_values_limit: usize,
    array_page_size: usize,
    null_pages_cache: HashMap<(DataType, usize), Rc<CompressedPage>>,
    data_page_size_limit: usize,
}


fn validate_schema(schema: &SchemaDescriptor, index_cols: usize) -> Result<()> {
    if schema.fields().len() < index_cols {
        return Err(StoreError::InvalidParameter(format!(
            "Index columns count {} is greater than schema fields count {}",
            index_cols,
            schema.fields().len()
        )));
    }

    for col_id in 0..index_cols {
        match &schema.fields()[col_id] {
            ParquetType::PrimitiveType(pt) => if pt.field_info.repetition == Repetition::Required {
                return Err(StoreError::NotYetSupported(format!("index field {} has repetition which doesn't supported", pt.field_info.name)));
            }
            ParquetType::GroupType { field_info, .. } => return Err(StoreError::NotYetSupported(format!("index group field {} doesn't supported", field_info.name)))
        }
        if let ParquetType::PrimitiveType(pt) = &schema.fields()[col_id] {
            if pt.field_info.repetition == Repetition::Required {
                return Err(StoreError::NotYetSupported(format!("index field {} has repetition which doesn't supported", pt.field_info.name)));
            }
        }
    }

    for root_field in schema.fields().iter() {
        match root_field {
            ParquetType::PrimitiveType(_) => {}
            ParquetType::GroupType { fields, logical_type, .. } => {
                match logical_type {
                    None => return Err(StoreError::NotYetSupported(format!("group field {} doesn't have logical type", root_field.get_field_info().name))),
                    Some(GroupLogicalType::List) => {}
                    _ => return Err(StoreError::NotYetSupported(format!("group  field {} has unsupported logical type. Only List is supported", root_field.get_field_info().name)))
                }

                match fields.len() {
                    0 => return Err(StoreError::InvalidParameter(format!("invalid group type {}", root_field.get_field_info().name))),
                    1 => {
                        if let ParquetType::GroupType { fields, .. } = &fields[0] {
                            if let ParquetType::GroupType { .. } = &fields[0] {
                                return Err(StoreError::NotYetSupported(format!("group field {} has a complex type, only single primitive field is supported", root_field.get_field_info().name)));
                            }
                        }
                    }
                    _ => return Err(StoreError::NotYetSupported(format!("field {} has a group type with multiple fields, only single primitive field is supported", root_field.get_field_info().name)))
                }
            }
        }
    }

    Ok(())
}

pub struct IndexChunk {
    pub cols: Vec<Vec<CompressedPage>>,
    reorder: MergeReorder,
}

impl IndexChunk {
    pub fn new(cols: Vec<Vec<CompressedPage>>, reorder: MergeReorder) -> Self {
        Self { cols, reorder }
    }
}

#[derive(Debug, Clone)]
pub enum MergeReorder {
    PickFromStream(usize, usize),
    // first vector - stream_id to pick from, second vector - streams which were merged
    Merge(Vec<usize>, Vec<usize>),
}

impl<R, W> FileMerger<R, W>
    where
        R: Read + Seek,
        W: Write,
{
    pub fn try_new(
        mut page_streams: Vec<CompressedPageIterator<R>>,
        writer: W,
        index_cols: usize,
        data_page_size_limit: usize,
        row_group_values_limit: usize,
        array_page_size: usize,
        name: String,
    ) -> Result<Self> {
        let streams_n = page_streams.len();
        let schemas = page_streams
            .iter()
            .map(|ps| ps.schema())
            .collect::<Vec<_>>();
        let schema = try_merge_schemas(schemas, name)?;
        validate_schema(&schema, index_cols)?;

        let opts = WriteOptions {
            write_statistics: true,
            version: Version::V2,
        };
        let seq_writer = FileSeqWriter::new(writer, schema.clone(), opts, None);
        let index_cols = (0..index_cols)
            .into_iter()
            .map(|idx| schema.columns()[idx].to_owned())
            .collect::<Vec<_>>();

        Ok(Self {
            index_cols,
            schema,
            page_streams,
            tmp_arrays: (0..streams_n).into_iter().map(|_| HashMap::new()).collect(),
            tmp_array_idx: (0..streams_n).into_iter().map(|_| HashMap::new()).collect(),
            sorter: BinaryHeap::new(),
            merge_queue: Vec::with_capacity(100),
            result_buffer: VecDeque::with_capacity(10),
            writer: seq_writer,
            row_group_values_limit,
            array_page_size,
            null_pages_cache: HashMap::new(),
            data_page_size_limit,
        })
    }

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

    fn make_null_pages(
        &mut self,
        cd: &ColumnDescriptor,
        num_rows: usize,
        data_pagesize_limit: usize,
    ) -> Result<Vec<CompressedPage>> {
        let field = column_to_field(cd);
        let arr = new_null_array(field.data_type, num_rows);
        Ok(array_to_pages_simple(arr, cd.base_type.clone(), data_pagesize_limit)?)
    }

    pub fn merge(&mut self) -> Result<()> {
        for stream_id in 0..self.page_streams.len() {
            if let Some(chunk) = self.next_stream_index_chunk(stream_id)? {
                self.sorter.push(chunk);
            }
        }

        while let Some(chunks) = self.next_index_chunk()? {
            for col_id in 0..self.index_cols.len() {
                for chunk in chunks.iter() {
                    for page in chunk.0.cols[col_id].iter() {
                        self.writer.write_page(page)?;
                    }
                }
                self.writer.end_column()?;
            }

            // todo avoid cloning
            let cols = self
                .schema
                .columns()
                .iter()
                .skip(self.index_cols.len())
                .map(|v| v.to_owned())
                .collect::<Vec<_>>();

            for col in cols.iter() {
                for chunk in chunks.iter() {
                    match &chunk.1 {
                        MergeReorder::PickFromStream(stream_id, num_rows) => {
                            let pages = if self.page_streams[*stream_id].contains_column(&col.path_in_schema) {
                                self.page_streams[*stream_id]
                                    .next_chunk(&col.path_in_schema)?
                                    .unwrap()
                            } else {
                                self.make_null_pages(&col, *num_rows, self.data_page_size_limit)?
                            };
                            for page in pages {
                                self.writer.write_page(&page)?;
                            }
                        }
                        MergeReorder::Merge(reorder, streams) => {
                            let pages = self.merge_data(&col, reorder, streams)?;
                            for page in pages {
                                self.writer.write_page(&page)?;
                            }
                        }
                    }
                }

                self.writer.end_column()?;
            }
            self.writer.end_row_group()?;
        }

        self.writer.end(None)?;

        Ok(())
    }

    fn merge_data(
        &mut self,
        col: &ColumnDescriptor,
        reorder: &[usize],
        streams: &[usize],
    ) -> Result<Vec<CompressedPage>> {
        let dt = column_to_field(col).data_type;

        println!("col {:?} pt {:?}", col, dt.to_physical_type());
        let out = match dt.to_physical_type() {
            ArrowPhysicalType::Primitive(v) => {
                match v {
                    arrow2::types::PrimitiveType::Int64 => merge_arrays!(self, TmpArray::Int64,Int64Array,MutablePrimitiveArray<i64>,col,reorder,streams),
                    _ => unimplemented!()
                }
            }
            ArrowPhysicalType::Utf8 => {
                merge_arrays!(self, TmpArray::Utf8,Utf8Array<i32>,MutableBinaryArray<i32>,col,reorder,streams)
            }
            ArrowPhysicalType::List => {
                match dt {
                    DataType::List(f) => {
                        match f.data_type.to_physical_type() {
                            ArrowPhysicalType::Primitive(v) => {
                                match v {
                                    arrow2::types::PrimitiveType::Int64 => merge_list_arrays!(self, i32, TmpArray::ListInt64,Int64Array,MutableListArray<i32,MutablePrimitiveArray<i64>>,col,reorder,streams),
                                    _ => unimplemented!()
                                }
                            }
                            ArrowPhysicalType::Utf8 => {
                                merge_list_arrays!(self, i32, TmpArray::ListUtf8,Utf8Array<i32>,MutableListArray<i32,MutableBinaryArray<i32>>,col,reorder,streams)
                            }
                            _ => unimplemented!(),
                        }
                    }
                    _ => unimplemented!()
                }
            }
            _ => unimplemented!("{:?}", dt.to_physical_type())
        };

        Ok(array_to_pages_simple(out, col.base_type.clone(), self.data_page_size_limit)?)
    }

    fn merge_queue(&mut self) -> Result<()> {
        println!("merge queue {:?}", self.merge_queue);
        let mut buf = vec![];
        let arrow_chunks = self
            .merge_queue
            .drain(..)
            .map(|chunk| chunk.to_arrow_chunk(&mut buf, &self.index_cols))
            .collect::<Result<Vec<_>>>()?;

        let mut streams = arrow_chunks.iter().map(|chunk| chunk.stream).collect::<Vec<_>>();
        streams.dedup();
        println!("to merge {:?}", arrow_chunks);
        let merged_chunks = merge(arrow_chunks, self.array_page_size)?;

        for chunk in merged_chunks {
            println!("merged chunk {:?}",chunk);
            let pages_chunk = PagesChunk::from_arrow(chunk.arrs.as_slice(), &self.index_cols)?;
            let merged_chunk = MergedPagesChunk::new(pages_chunk, MergeReorder::Merge(chunk.reorder, streams.clone()));
            self.result_buffer.push_back(merged_chunk);
        }

        Ok(())
    }

    fn next_index_chunk(
        &mut self,
    ) -> Result<Option<Vec<MergedPagesChunk>>> {
        while let Some(chunk) = self.sorter.pop() {
            println!("pop {:#?}", chunk);
            if let Some(next) = self.next_stream_index_chunk(chunk.stream)? {
                println!("pop next {:#?}", next);
                self.sorter.push(next);
            }

            self.merge_queue.push(chunk);
            while check_intersection(&self.merge_queue, self.sorter.peek()) {
                println!("queue intersects with {:?}", self.sorter.peek());
                let next = self.sorter.pop().unwrap();
                if let Some(row) = self.next_stream_index_chunk(next.stream)? {
                    println!("pop next {:?}", row);
                    self.sorter.push(row);
                }
                self.merge_queue.push(next);
            }

            if self.merge_queue.len() > 1 {
                self.merge_queue()?;
            } else {
                println!("push to result");

                let chunk = self.merge_queue.pop().unwrap();
                let num_vals = chunk.num_values();
                let chunk_stream = chunk.stream;
                self.result_buffer.push_back(MergedPagesChunk::new(chunk, MergeReorder::PickFromStream(chunk_stream, num_vals)));
            }

            println!("result {:?}", self.result_buffer);
            if let Some(res) = self.try_drain_result(self.row_group_values_limit) {
                return Ok(Some(res));
            }
        }

        Ok(self.try_drain_result(1))
    }

    fn try_drain_result(
        &mut self,
        values_limit: usize,
    ) -> Option<Vec<MergedPagesChunk>> {
        println!("try drain");
        if self.result_buffer.is_empty() {
            println!("empty");
            return None;
        }
        let mut res = vec![];
        let first = self.result_buffer.pop_front().unwrap();
        let mut values_limit = values_limit as i64;
        values_limit -= first.num_values() as i64;
        res.push(first);
        while values_limit > 0 && !self.result_buffer.is_empty() {
            println!("try");
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