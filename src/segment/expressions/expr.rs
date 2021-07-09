use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::{ColumnarValue, PhysicalExpr};
use std::sync::Arc;
use arrow::array::ArrayRef;

pub trait Expr<T> {
    fn evaluate(&self, batch: &RecordBatch, row_id: usize) -> T;
}