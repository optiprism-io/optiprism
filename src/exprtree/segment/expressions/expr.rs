use arrow::array::ArrayRef;
use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::{ColumnarValue, PhysicalExpr};
use std::sync::Arc;

pub trait Expr<T> {
    fn evaluate(&self, batch: &RecordBatch, row_id: usize) -> T;
}
