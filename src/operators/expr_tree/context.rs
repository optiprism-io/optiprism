use arrow::record_batch::RecordBatch;

pub struct Context {
    pub batch: RecordBatch,
    pub row_id: usize,
}