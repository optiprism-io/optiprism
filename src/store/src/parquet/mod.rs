use parquet2::metadata::ColumnChunkMetaData;

pub struct SequentialWriter {}

impl SequentialWriter {
    pub fn new() -> Self {
        Self {}
    }

    pub fn start_row_group(&mut self, columns: &[ColumnChunkMetaData]) {}
}