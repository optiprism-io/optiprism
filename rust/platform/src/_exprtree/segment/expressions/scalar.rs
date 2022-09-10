use crate::exprtree::segment::expressions::expr::Expr;
use arrow::record_batch::RecordBatch;

pub struct Scalar<T> {
    value: T,
}

impl<T> Scalar<T> {
    pub fn new(value: T) -> Self {
        Self { value }
    }
}

impl<T> Expr<T> for Scalar<T>
where
    T: Copy,
{
    fn evaluate(&self, _: &RecordBatch, _: usize) -> T {
        self.value
    }
}
