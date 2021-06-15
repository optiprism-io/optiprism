use arrow::record_batch::RecordBatch;
use arrow::datatypes::Schema;
use std::sync::Arc;
use datafusion::{
    error::{Result},
};

pub struct Context {
    pub batch: RecordBatch,
    pub row_id: usize,
}

impl Context {
    pub fn new_empty() -> Self {
        let schema = Arc::new(Schema::new(vec![]));
        let batch = RecordBatch::new_empty(schema.clone());
        Context {
            batch,
            row_id: 0,
        }
    }
}