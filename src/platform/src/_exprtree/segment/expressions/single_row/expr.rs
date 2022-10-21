use arrow::array::ArrayRef;
use arrow::record_batch::RecordBatch;
use datafusion::error::Result as DatafusionResult;
use datafusion::physical_plan::{ColumnarValue, PhysicalExpr};
use std::sync::Arc;

pub trait Expr {
    fn evaluate(&self, batch: &RecordBatch, row_id: usize) -> DatafusionResult<bool>;
}
