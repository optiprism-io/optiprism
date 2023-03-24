use std::any::Any;
use std::cmp;
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::io::Read;
use std::io::Seek;
use std::io::Write;
use std::marker::PhantomData;
use std::ops::Range;
use std::ops::RangeBounds;
use std::rc::Rc;

use arrow2::array::{Array, ListArray, MutableArray, MutableListArray, MutablePrimitiveArray, new_null_array, PrimitiveArray, TryExtend, TryPush};
use arrow2::array::BinaryArray;
use arrow2::array::BooleanArray;
use arrow2::array::FixedSizeBinaryArray;
use arrow2::array::Float32Array;
use arrow2::array::Float64Array;
use arrow2::array::Int32Array;
use arrow2::array::Int64Array;
use arrow2::datatypes::{DataType, Field};
use arrow2::ffi::mmap::slice;
use arrow2::io::parquet::read::column_iter_to_arrays;
use arrow2::io::parquet::read::schema::parquet_to_arrow_schema;
use parquet2::metadata::ColumnDescriptor;
use parquet2::metadata::SchemaDescriptor;
use parquet2::page::CompressedDataPage;
use parquet2::page::CompressedPage;
use parquet2::page::Page;
use parquet2::read::decompress;
use parquet2::schema::types::FieldInfo;
use parquet2::schema::types::ParquetType;
use parquet2::schema::types::PhysicalType;
use parquet2::schema::types::PrimitiveType;
use parquet2::write::FileSeqWriter;
use parquet2::write::Version;
use parquet2::write::WriteOptions;

use crate::error::Result;
use crate::error::StoreError;
use crate::parquet::arrow::merge;
use crate::parquet::arrow::ArrowRow;
use crate::parquet::parquet::{array_to_page};
use crate::parquet::parquet::check_intersection;
use crate::parquet::parquet::data_page_to_array;
use crate::parquet::parquet::data_pages_to_arrays;
use crate::parquet::parquet::ColumnPath;
use crate::parquet::parquet::CompressedDataPagesColumns;
use crate::parquet::parquet::CompressedDataPagesRow;
use crate::parquet::parquet::CompressedPageIterator;
use crate::parquet::schema::try_merge_schemas;

enum OwnArray {
    Int64(Int64Array),
    List(ListArray<i32>),
}

pub struct FileMerger<R, W>
    where
        R: Read,
        W: Write,
{
    index_cols: Vec<ColumnDescriptor>,
    schema: SchemaDescriptor,
    page_streams: Vec<CompressedPageIterator<R>>,
    current_arrs: Vec<HashMap<ColumnPath, OwnArray>>,
    current_arrs_idx: Vec<HashMap<ColumnPath, usize>>,
    sorter: BinaryHeap<CompressedDataPagesRow>,
    merge_queue: Vec<CompressedDataPagesRow>,
    result: (Vec<Vec<CompressedPage>>, Vec<MergeReorder>),
    writer: FileSeqWriter<W>,
    pages_per_chunk: usize,
    page_size: usize,
    null_pages_cache: HashMap<(DataType, usize), Rc<CompressedPage>>,
}

#[derive(Debug, Clone)]
pub enum MergeReorder {
    // stream_id and
    StreamPage(usize, usize),
    // first vector - stream_id to pick from, second vector - streams which are merged
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
        page_size: usize,
        pages_per_chunk: usize,
        name: String,
    ) -> Result<Self> {
        let streams_n = page_streams.len();
        let schemas = page_streams
            .iter()
            .map(|ps| ps.schema())
            .collect::<Vec<_>>();
        let schema = try_merge_schemas(schemas, name)?;
        let opts = WriteOptions {
            write_statistics: true,
            version: Version::V2,
        };
        let seq_writer = FileSeqWriter::new(writer, schema.clone(), opts, None);
        let index_cols = (0..index_cols)
            .into_iter()
            .map(|idx| schema.columns()[idx].to_owned())
            .collect::<Vec<_>>();

        let col_res = (0..index_cols.len())
            .into_iter()
            .map(|_| Vec::with_capacity(pages_per_chunk))
            .collect::<Vec<_>>();

        Ok(Self {
            index_cols,
            schema,
            page_streams,
            current_arrs: (0..streams_n).into_iter().map(|_| HashMap::new()).collect(),
            current_arrs_idx: (0..streams_n).into_iter().map(|_| HashMap::new()).collect(),
            sorter: BinaryHeap::new(),
            merge_queue: Vec::with_capacity(100),
            result: (col_res, Vec::with_capacity(pages_per_chunk)),
            writer: seq_writer,
            pages_per_chunk,
            page_size,
            null_pages_cache: HashMap::new(),
        })
    }

    fn make_null_page(
        &mut self,
        cd: &ColumnDescriptor,
        num_rows: usize,
    ) -> Result<Rc<CompressedPage>> {
        let field = parquet_to_arrow_schema(vec![cd.base_type.clone()].as_slice()).pop().unwrap();
        let cache_key = (field.data_type.clone(), num_rows);
        if let Some(page) = self.null_pages_cache.get(&cache_key) {
            return Ok(Rc::clone(page));
        }
        let arr = new_null_array(field.data_type, num_rows);

        let page = array_to_page(arr, cd.base_type.clone())?;
        self.null_pages_cache.insert(cache_key.clone(), Rc::new(page));

        let rc = self.null_pages_cache.get(&cache_key).unwrap();
        Ok(Rc::clone(rc))
    }

    pub fn merge(&mut self) -> Result<()> {
        for stream_id in 0..self.page_streams.len() {
            if let Some(row) = self.next_compressed_row(stream_id)? {
                self.sorter.push(row);
            }
        }

        while let Some((index_cols, reorder)) = self.next_index_chunk()? {
            for col in index_cols.into_iter() {
                for page in col.into_iter() {
                    self.writer.write_page(&page)?;
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
                for reorder in reorder.iter() {
                    match reorder {
                        MergeReorder::StreamPage(stream_id, num_rows) => {
                            if self.page_streams[*stream_id].contains_column(&col.path_in_schema) {
                                let page = self.page_streams[*stream_id]
                                    .next_page(&col.path_in_schema)?
                                    .unwrap();
                                self.writer.write_page(&page)?;
                            } else {
                                let null_page = self.make_null_page(&col, *num_rows)?;
                                self.writer.write_page(&null_page)?;
                            }
                        }
                        MergeReorder::Merge(reorder, streams) => {
                            let page = self.merge_data(&col, reorder, streams)?;
                            self.writer.write_page(&page)?;
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
    ) -> Result<CompressedPage> {
        let col_path: ColumnPath = col.path_in_schema.clone();

        println!("cp {:#?}", col);
        let mut buf = vec![];
        let col_exist_per_stream = self
            .page_streams
            .iter()
            .enumerate()
            .map(|(stream_id, stream)| {
                streams.contains(&stream_id) && stream.contains_column(&col_path)
            })
            .collect::<Vec<bool>>();

        let mut arrs = self
            .current_arrs
            .iter_mut()
            .enumerate()
            .map(|(stream_id, cols)| {
                if streams.contains(&stream_id) {
                    match cols.remove(&col_path) {
                        None => None,
                        Some(v) => {
                            if let OwnArray::List(arr) = v {
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

        let mut arrs_idx: Vec<usize> = self
            .current_arrs_idx
            .iter()
            .map(|cols| match cols.get(&col_path) {
                Some(idx) => *idx,
                None => 0,
            })
            .collect();

        let mut out = MutableListArray::<i32, MutablePrimitiveArray<i64>>::with_capacity(reorder.len());
        for idx in 0..reorder.len() {
            let stream_id = reorder[idx];
            if !col_exist_per_stream[stream_id] {
                out.push_null();
                continue;
            }

            if arrs[stream_id].is_none() {
                let page = self.page_streams[stream_id].next_page(&col_path)?.unwrap();
                if let CompressedPage::Data(page) = page {
                    let any_arr = data_page_to_array(page, &col, &mut buf)?;
                    println!("any arr {:#?}", any_arr.data_type());
                    let list_arr = any_arr.as_any().downcast_ref::<ListArray<i32>>().unwrap().clone();
                    arrs[stream_id] = Some(list_arr.clone());
                    arrs_idx[stream_id] = 0;
                }
            }

            let arr = arrs[stream_id].as_ref().unwrap();
            if arr.is_null(arrs_idx[stream_id]) {
                out.push_null();
            } else {
                // todo fix with array downcast
                // let b = arr.value(arrs_idx[stream_id]).as_any().downcast_ref::<PrimitiveArray<i64>>().unwrap().clone();
                let all_vals = arr.values().as_any().downcast_ref::<PrimitiveArray<i64>>().unwrap().clone();

                let (start, end) = arr.offsets().start_end(arrs_idx[stream_id]);
                let length = end - start;
                // safety: the invariant of the struct
                let g = all_vals.sliced(start, length);
                    out.try_push(Some(g))?;
                // out.try_extend(Some(arr.value(arrs_idx[stream_id])));
            }

            if arrs_idx[stream_id] == arr.len() - 1 {
                arrs[stream_id] = None;
                arrs_idx[stream_id] = 0;
            } else {
                arrs_idx[stream_id] += 1;
            }
        }

        for (stream_id, maybe_arr) in arrs.into_iter().enumerate() {
            if let Some(arr) = maybe_arr {
                self.current_arrs[stream_id].insert(col_path.clone(), OwnArray::List(arr));
            }
        }

        for (stream_id, idx) in arrs_idx.into_iter().enumerate() {
            self.current_arrs_idx[stream_id].insert(col_path.clone(), idx);
        }

        let page = match array_to_page(
            out.as_box(),
            col.base_type.clone(),
        )? {
            CompressedPage::Data(v) => v,
            CompressedPage::Dict(_) => unimplemented!(),
        };

        Ok(CompressedPage::Data(page))
    }

    fn merge_queue(&mut self) -> Result<()> {
        println!("merge queue {:?}", self.merge_queue);
        let mut buf = vec![];
        let queue = self
            .merge_queue
            .drain(..)
            .map(|row| row.to_arrow_row(&mut buf))
            .collect::<Result<Vec<_>>>()?;

        let merge_result = merge(queue, self.page_size)?;
        println!("merge result {:?}", merge_result);

        for (arrow_row, reorder, streams) in merge_result {
            let types = self
                .index_cols
                .iter()
                .map(|cd| cd.descriptor.primitive_type.clone())
                .collect::<Vec<_>>();
            let row = CompressedDataPagesRow::from_arrow_row(arrow_row, types, vec![])?;
            self.push_to_result(row, MergeReorder::Merge(reorder, streams))
        }

        Ok(())
    }

    fn next_compressed_row(&mut self, stream_id: usize) -> Result<Option<CompressedDataPagesRow>> {
        let mut stream = &mut self.page_streams[stream_id];
        let mut pages: Vec<CompressedPage> = Vec::with_capacity(self.index_cols.len());
        for col in self.index_cols.iter() {
            match stream.next_page(&col.path_in_schema)? {
                None => return Ok(None),
                Some(page) => pages.push(page),
            }
        }

        Ok(Some(CompressedDataPagesRow::new_from_pages(
            pages, stream_id,
        )))
    }

    fn push_to_result(&mut self, row: CompressedDataPagesRow, reorder: MergeReorder) {
        for (col_id, page) in row.pages.into_iter().enumerate() {
            self.result.0[col_id].push(CompressedPage::Data(page));
        }
        self.result.1.push(reorder);
    }

    fn next_index_chunk(
        &mut self,
    ) -> Result<Option<(Vec<Vec<CompressedPage>>, Vec<MergeReorder>)>> {
        while let Some(row) = self.sorter.pop() {
            println!("pop {:?}", row);
            if let Some(next) = self.next_compressed_row(row.stream)? {
                println!("pop next {:?}", next);
                self.sorter.push(next);
            }

            self.merge_queue.push(row);
            while check_intersection(&self.merge_queue, self.sorter.peek()) {
                println!("queue intersects with {:?}", self.sorter.peek());
                let next = self.sorter.pop().unwrap();
                if let Some(row) = self.next_compressed_row(next.stream)? {
                    println!("pop next {:?}", row);
                    self.sorter.push(row);
                }
                self.merge_queue.push(next);
            }

            if self.merge_queue.len() > 1 {
                self.merge_queue()?;
            } else {
                println!("push to result");

                let row = self.merge_queue.pop().unwrap();
                let num_vals = row.pages[0].num_values();
                let row_stream = row.stream;
                self.push_to_result(row, MergeReorder::StreamPage(row_stream, num_vals));
            }

            println!("result {:?}", self.result);
            if let Some(res) = self.try_drain_result(self.pages_per_chunk) {
                return Ok(Some(res));
            }
        }

        Ok(self.try_drain_result(1))
    }

    fn try_drain_result(
        &mut self,
        num: usize,
    ) -> Option<(Vec<Vec<CompressedPage>>, Vec<MergeReorder>)> {
        if self.result.0[0].is_empty() {
            return None;
        }
        let end = cmp::min(self.result.0[0].len(), num);
        let cols = self
            .result
            .0
            .iter_mut()
            .map(|v| v.drain(..end).collect::<Vec<_>>())
            .collect::<Vec<_>>();
        let reorder = self.result.1.drain(0..end).collect::<Vec<_>>();

        return Some((cols, reorder));

        None
    }
}
