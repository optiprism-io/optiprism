use crate::exprtree::segment::expressions::single_row::expr::Expr;
use arrow::array::ArrayRef;
use arrow::record_batch::RecordBatch;
use datafusion::error::Result as DatafusionResult;

pub struct True;

impl True {
    pub fn new() -> Self {
        True {}
    }
}

impl Expr for True {
    fn evaluate(&self, _: &RecordBatch, _: usize) -> DatafusionResult<bool> {
        Ok(true)
    }
}

pub struct False;

impl False {
    pub fn new() -> Self {
        False {}
    }
}

impl Expr for False {
    fn evaluate(&self, _: &RecordBatch, _: usize) -> DatafusionResult<bool> {
        Ok(false)
    }
}
