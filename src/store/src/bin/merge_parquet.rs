use std::collections::BinaryHeap;
use std::io::Read;
use std::io::Write;

use arrow2::array::Array;
use arrow2::array::PrimitiveArray;
use parquet2::metadata::ColumnDescriptor;
use parquet2::metadata::SchemaDescriptor;
use parquet2::page::CompressedPage;
use parquet2::read::PageReader;
use parquet2::write::FileSeqWriter;
use rayon::prelude::*;
use store::error::Result;

struct ReorderSlices {}

impl PagesRow {
    pub fn from_parquet(
        stream: usize,
        columns: Vec<CompressedPage>,
        reorder: Option<ReorderSlices>,
    ) -> Self {
        Self {
            stream: Some(stream),
            data: PagesData::Parquet(columns),
            reorder,
        }
    }

    pub fn from_arrow(columns: Vec<Box<dyn Array>>, reorder: Option<ReorderSlices>) -> Self {
        Self {
            stream: None,
            data: PagesData::Arrow(columns),
            reorder,
        }
    }
}

type MergeTask = Vec<MergeItem>;

struct MergeResult(Vec<(ArrowRow, Option<ReorderSlices>)>);

type MergeItem = (ArrowRow, Option<usize>, Option<ReorderSlices>);


enum RowData {
    Arrow(Vec<Box<dyn Array>>),
    Parquet(Vec<CompressedPage>),
}

struct ColumnsRow {
    stream: Option<usize>,
    reorder: Option<ReorderSlices>,
    data: RowData,
}

trait RowCmp: Eq + PartialEq + PartialOrd + Ord;

enum SortItem {
    Merged(ArrowRow, Option<ReorderSlices>),
    Unmerged(usize, CompressedPagesRow),
}

impl SortItem {
    pub fn min_max(&self) -> (Box<dyn RowCmp>, Box<dyn RowCmp>) {}
}

impl SortItem {
    pub fn new_merged(row: ArrowRow, reorder: Option<ReorderSlices>) -> Self {
        Self::Merged(row, reorder)
    }

    pub fn new_unmerged(stream: usize, row: CompressedPagesRow) -> Self {
        Self::Unmerged(stream, row)
    }
}

impl From<MergeResult> for SortItem {
    fn from(result: MergeResult) -> Self {
        let (row, reorder) = result.0.into_iter().next().unwrap();
        Self::new_merged(row, reorder)
    }
}

struct ArrowRow {
    cols: Vec<Box<dyn Array>>,
    min: Box<dyn RowCmp>,
    max: Box<dyn RowCmp>,
}

struct CompressedPagesRow {
    cols: Vec<CompressedPage>,
    min: Box<dyn RowCmp>,
    max: Box<dyn RowCmp>,
}

type CompressedPagesColumns = Vec<Vec<CompressedPage>>;

struct CompressedPagesRowIterator {}

impl Iterator for CompressedPagesRowIterator {
    type Item = Result<CompressedPagesRow>;

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
    sorter: BinaryHeap<SortItem>,
    sort_finished: bool,
    merge_queue: Vec<MergeTask>,
    merge_result: BinaryHeap<SortItem>,
    writer: FileSeqWriter<W>,
}

fn merge(task: Vec<Vec<Box<dyn Array>>>) -> Result<MergeResult> {}

impl<R, W> FileMerger<R, W>
    where
        R: Read,
        W: Write,
{
    pub fn merge(readers: Vec<R>, schemas: Vec<SchemaDescriptor>, writer: W) -> Result<()> {
        let mut m = Self {
            readers,
            schemas,
            writer,
        };

        m._merge()
    }

    fn next_index_row(&mut self) -> Result<Option<(CompressedPagesRow, Option<ReorderSlices>)>> {
        if self.sort_finished {
            return Ok(None);
        }

        loop {
            // loop over all streams and push the next page into the sorter if it is not already there
            for (stream_idx, stream) in self.index_pages_stream.iter_mut().enumerate() {
                let mut in_sorter = false;
                for item in self.sorter.iter() {
                    if let SortItem::Unmerged(stream, _) = item {
                        if *stream == stream_idx {
                            in_sorter = true;
                            break;
                        }
                    }
                }

                // if not in sorter, push the next page from the stream
                if !in_sorter {
                    // take the next page from the stream
                    match stream.next() {
                        Some(compressed_row) => {
                            let item = SortItem::new_unmerged(stream_idx, compressed_row?);
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

                let res: Vec<MergeResult> = self.merge_queue
                    .par_drain(..)
                    .map(|v| merge(v))
                    .collect::<Result<_>>()?;

                res.into_iter().for_each(|v| {
                    v.0.into_iter()
                        .for_each(|(row, reorder)| self.merge_result.push(SortItem::new_merged(row, reorder)))
                });

                self.sorter.push(self.merge_result.pop().unwrap().into());
            }

            let mut out = Vec::new();
            let first = sorter.pop().unwrap();
            let (mut min, mut max) = first.min_max();
            out.push(first);

            while !self.sorter.is_empty() {
                match self.sorter.peek() {
                    None => break,
                    Some(i) => {
                        let (int_min, int_max) = i.min_max();
                        if int_min >= min && int_min <= max {
                            out.push(*int);
                            if int_max > max {
                                max = int_max;
                            }
                            sorter.pop();
                        } else {
                            break;
                        }
                    }
                }
            }

            if out.len() == 1 {
                return Ok(out[0]);
            } else {
                println!("merge {:?} to ({min},{max})", out);
                self.merge_queue.push(out);
                // it is time to merge
                if self.merge_queue.len() == self.parallel {
                    println!("merge of queue: {:?}", self.merge_queue);
                    let res: Vec<MergeResult> = merge_queue
                        .par_drain(..)
                        .map(|v| merge(v))
                        .collect::<Result<_>>()?;

                    res.into_iter().for_each(|v| {
                        v.0.into_iter()
                            .for_each(|(row, reorder)| merge_result.push(SortItem::new_merged(row, reorder)))
                    });
                }
            }
        }
    }
    fn next_index_chunk(
        &mut self,
        pages_per_chunk: usize,
    ) -> Result<Option<(CompressedPagesColumns, Vec<Option<ReorderSlices>>)>> {
        if self.sort_finished {
            return Ok(None);
        }

        let mut result: CompressedPagesColumns = vec![Vec::with_capacity(pages_per_chunk); self.index_pages_stream.len()];
        let mut slices: Vec<Option<ReorderSlices>> = Vec::new();
        for _ in 0..pages_per_chunk {
            match self.next_index_row()? {
                None => {
                    break;
                }
                Some((row, reorder)) => {
                    result.push(row.cols);
                    slices.push(reorder);
                }
            }
        }

        if result.is_empty() {
            return Ok(None);
        }

        Ok(Some((result, slices)))
    }
    fn next_data_chunk(
        &mut self,
        col: &ColumnDescriptor,
        slices: &[ReorderSlices],
    ) -> Result<Vec<CompressedPage>> {
        todo!()
    }

    fn _merge(&mut self) -> Result<()> {
        let pages_per_chunk = 2;
        loop {
            let maybe_chunk = self.next_index_chunk(pages_per_chunk)?;
            if maybe_chunk.is_none() {
                break;
            }
            let (cols, slices) = maybe_chunk.unwrap();
            for pages in cols {
                for page in pages.into_iter() {
                    self.writer.write_page(&page)?;
                }
                self.writer.end_column()?;
            }

            for col in self.data_columns.iter() {
                for page in self.next_data_chunk(col, &slices)? {
                    self.writer.write_page(&page)?;
                }
                self.writer.end_column()?;
            }

            self.writer.end_row_group()?;
        }

        self.writer.end(None)?;

        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    todo!()
}
