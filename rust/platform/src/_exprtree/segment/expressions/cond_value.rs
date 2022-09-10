use super::expr::Expr;
use arrow::array::Int8Array;
use arrow::record_batch::RecordBatch;

pub struct Case<T> {
    when: Box<dyn Expr<bool>>,
    then: Box<dyn Expr<Option<T>>>,
}

impl<T> Case<T> {
    pub fn new(when: Box<dyn Expr<bool>>, then: Box<dyn Expr<Option<T>>>) -> Self {
        Case { when, then }
    }
}

impl<T> Expr<Option<T>> for Case<T> {
    fn evaluate(&self, batch: &RecordBatch, row_id: usize) -> Option<T> {
        match self.when.evaluate(batch, row_id) {
            true => self.then.evaluate(batch, row_id),
            false => None,
        }
    }
}
