use std::fmt::{Debug, Display};
use arrow::array::ArrayRef;
use arrow::record_batch::RecordBatch;
use datafusion_expr::{ColumnarValue, Operator};
use crate::error::Result;

// pub mod binary_op;
pub mod count;
mod boolean_op;
// pub mod sequence;
// pub mod sum;
pub mod aggregate;
// mod test_value;
// mod boolean_op;

pub trait Expr: Send + Sync + Display + Debug {
    fn evaluate(&mut self, spans: &[usize], batch: &RecordBatch, is_last: bool) -> Result<Option<Vec<i64>>>;
}