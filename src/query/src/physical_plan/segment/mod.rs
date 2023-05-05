use std::fmt::{Debug, Display};
use arrow::record_batch::RecordBatch;
use crate::error::Result;

pub mod binary_op;
// pub mod count;
// pub mod sequence;
// pub mod sum;
// mod test_value;
// mod boolean_op;

pub trait Expr: Send + Sync + Display + Debug {
    fn evaluate(&self, batch: &RecordBatch) -> Result<bool>;
}