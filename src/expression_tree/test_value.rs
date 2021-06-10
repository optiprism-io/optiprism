use crate::expression_tree::expr::Expr;
use arrow::array::ArrayRef;
use arrow::record_batch::RecordBatch;

pub struct True;

impl True {
    pub fn new() -> Self {
        True {}
    }
}

impl Expr for True {
    fn evaluate(&self, _:  &[&RecordBatch]) -> bool {
        true
    }
}

pub struct False;

impl False {
    pub fn new() -> Self {
        False {}
    }
}

impl Expr for False {
    fn evaluate(&self, _:  &[&RecordBatch]) -> bool {
        false
    }
}