use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::{ColumnarValue, PhysicalExpr};
use std::sync::Arc;
use arrow::array::ArrayRef;
use datafusion::error::{Result as DatafusionResult};

pub trait Expr {
    fn evaluate(&self, batch: &RecordBatch, row_id:usize) -> DatafusionResult<bool>;
}