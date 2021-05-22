use crate::expression_tree::expr::{Expr};
use crate::expression_tree::context::Context;
use arrow::record_batch::RecordBatch;

struct Count {
    expr: Box<dyn Expr<bool>>,
}

impl Expr<usize> for Count {
    fn evaluate(&self, batch: &RecordBatch, row_id: usize) -> usize {
        match self.expr.evaluate(batch, row_id) {
            true => {
                1
            }
            false => {
                0
            }
        }
    }
}