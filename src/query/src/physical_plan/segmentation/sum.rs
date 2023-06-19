use arrow::record_batch::RecordBatch;

use crate::error::Result;

pub trait Expr {
    fn evaluate(&mut self, batch: &RecordBatch) -> Result<Vec<usize>>;
    fn finalize(&mut self) -> Result<Vec<usize>>;
}
