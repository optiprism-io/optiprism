use crate::expression_tree::context::Context;
use arrow::record_batch::RecordBatch;

pub trait Expr<T> {
    fn evaluate(&self, batch: &RecordBatch, row_id: usize) -> T;
}