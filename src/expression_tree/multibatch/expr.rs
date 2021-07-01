use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::{ColumnarValue, PhysicalExpr};
use std::sync::Arc;
use arrow::array::ArrayRef;
use datafusion::error::{Result as DatafusionResult};
use std::fmt::{Display, Debug};

pub trait Expr: Send + Sync + Display + Debug {
    fn evaluate(&self, batches: &[RecordBatch]) -> DatafusionResult<bool>;
}