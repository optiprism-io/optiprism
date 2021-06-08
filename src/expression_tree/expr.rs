use crate::expression_tree::context::Context;
use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::{ColumnarValue, PhysicalExpr};
use std::sync::Arc;
use arrow::array::ArrayRef;

pub trait Expr<T> {
    fn evaluate(&self, batch: &[ArrayRef], row_id: usize) -> T;
}