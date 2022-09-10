use crate::exprtree::segment::expressions::multibatch::expr::Expr;
use arrow::array::ArrayRef;
use arrow::record_batch::RecordBatch;
use datafusion::error::Result as DatafusionResult;

#[derive(Debug)]
pub struct True;

impl True {
    pub fn new() -> Self {
        True {}
    }
}

impl Expr for True {
    fn evaluate(&self, _: &[RecordBatch]) -> DatafusionResult<bool> {
        Ok(true)
    }
}

impl std::fmt::Display for True {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", "True")
    }
}

#[derive(Debug)]
pub struct False;

impl False {
    pub fn new() -> Self {
        False {}
    }
}

impl Expr for False {
    fn evaluate(&self, _: &[RecordBatch]) -> DatafusionResult<bool> {
        Ok(false)
    }
}

impl std::fmt::Display for False {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", "False")
    }
}
