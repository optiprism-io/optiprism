use std::collections::BinaryHeap;
use std::io::{Read, Write};
use arrow2::array::PrimitiveArray;
use parquet2::metadata::{ColumnDescriptor, SchemaDescriptor};
use parquet2::page::CompressedPage;
use parquet2::write::FileSeqWriter;
use store::error::Result;

struct PageReorderSlices {}

struct CompressedIndexPages {
    stream: usize,
    user_id: CompressedPage,
    timestamp: CompressedPage,
    reorder_slices: PageReorderSlices,
}

struct IndexChunkBuffer {
    user_id: Vec<CompressedPage>,
    timestamp: Vec<CompressedPage>,
    reorder_slices: Vec<PageReorderSlices>,
}

impl IndexChunkBuffer {
    pub fn new(pages_per_chunk: usize) -> Self {
        Self {
            user_id: Vec::with_capacity(pages_per_chunk),
            timestamp: Vec::with_capacity(pages_per_chunk),
            reorder_slices: Vec::with_capacity(pages_per_chunk),
        }
    }

    pub fn push_row(&mut self, pages: CompressedIndexPages) {
        self.user_id.push(pages.user_id);
        self.timestamp.push(pages.timestamp);
        self.reorder_slices.push(pages.reorder_slices);
    }

    pub fn drain(&mut self) -> (Vec<Vec<CompressedPage>>, Vec<PageReorderSlices>) {
        let user_id = self.user_id.drain(..).collect::<Vec<CompressedPage>>();
        let timestamp = self.timestamp.drain(..).collect::<Vec<CompressedPage>>();
        let slices = self.reorder_slices.drain(..).collect::<Vec<PageReorderSlices>>();
        (vec![user_id, timestamp], slices)
    }

    pub fn len(&self) -> usize {
        self.user_id.len()
    }
}

struct ArrowPages {
    stream: usize,
    user_id: PrimitiveArray<u64>,
    timestamp: PrimitiveArray<i64>,
}

enum Pages {
    Parquet(CompressedIndexPages),
    Arrow(ArrowPages),
}

struct MergeTask {
    pages: Vec<Pages>,
}

struct FileMerger<'a, R, W> where R: Read, W: Write {
    readers: Vec<R>,
    schemas: Vec<SchemaDescriptor>,
    schema: SchemaDescriptor,
    data_columns: Vec<ColumnDescriptor>,
    writer: FileSeqWriter<W>,
    merge_queue: Vec<MergeTask>,
    merge_result: BinaryHeap<Pages>,
}


impl<R, W> FileMerger<R, W> where R: Read, W: Write {
    pub fn merge(readers: Vec<R>, schemas: Vec<SchemaDescriptor>, writer: W) -> Result<()> {
        let mut m = Self { readers, schemas, writer };

        m._merge()
    }

    fn next_index_chunk(&mut self, pages_per_chunk: usize) -> Result<Option<(Vec<Vec<CompressedPage>>, Vec<PageReorderSlices>)>> {}
    fn next_data_chunk(&mut self, col: &ColumnDescriptor, slices: &[PageReorderSlices]) -> Result<Vec<CompressedPage>> {}

    fn _merge(&mut self) -> Result<()> {
        let pages_per_chunk = 2;
        let mut index_chunk_buffer = IndexChunkBuffer::new(pages_per_chunk);
        let mut finish = false;
        loop {
            let maybe_chunk = self.next_index_chunk(pages_per_chunk)?;
            if maybe_chunk.is_none() {
                break;
            }
            match self.next_index_chunk(pages_per_chunk)? {
                None => {}
                Some((cols, slices)) => {
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
                }
            }
            self.writer.end_row_group()?;
        }

        self.writer.end(None)?;

        Ok(())
    }
}

fn main() -> anyhow::Result<()> {}