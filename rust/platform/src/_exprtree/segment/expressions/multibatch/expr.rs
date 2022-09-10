use arrow::array::ArrayRef;
use arrow::record_batch::RecordBatch;
use datafusion::error::Result as DatafusionResult;
use datafusion::physical_plan::{ColumnarValue, PhysicalExpr};
use std::fmt::{Debug, Display};
use std::sync::Arc;

pub trait Expr: Send + Sync + Display + Debug {
    fn evaluate(&self, batches: &[RecordBatch]) -> DatafusionResult<bool>;
}
