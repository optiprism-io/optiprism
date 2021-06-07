use arrow::record_batch::RecordBatch;
use std::sync::Arc;

struct MultiBatch {
    batches: Vec<Arc<RecordBatch>>,
    from_id: usize,
    to_id: usize,
}

impl MultiBatch {
    fn new(batches: Vec<Arc<RecordBatch>>, from_id: usize, to_id: usize) -> MultiBatch {
        MultiBatch {
            batches:batches.clone(),
            from_id,
            to_id,
        }
    }
}