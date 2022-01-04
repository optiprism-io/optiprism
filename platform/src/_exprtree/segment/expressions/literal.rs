use crate::exprtree::segment::expressions::expr::Expr;
use arrow::record_batch::RecordBatch;

struct Literal<T> {
    value: T,
}

impl<T> Expr<T> for Literal<T>
where
    T: Copy,
{
    fn evaluate(&self, _: &RecordBatch, _: usize) -> T {
        return self.value;
    }
}
