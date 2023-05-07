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
// mod test_value;
// mod boolean_op;

trait Comparable: Eq + PartialEq + PartialOrd {}

pub trait Expr: Send + Sync + Display + Debug {
    fn evaluate(&mut self, spans: &[usize], batch: &RecordBatch, is_last: bool) -> Result<Option<Vec<i64>>>;
}

pub fn break_on_false(op: Operator) -> bool {
    match op {
        Operator::Eq | Operator::NotEq => false,
        Operator::Lt | Operator::LtEq => true,
        Operator::Gt | Operator::GtEq => false,
        _ => {
            panic!("unexpected")
        }
    }
}

pub fn break_on_true(op: Operator) -> bool {
    match op {
        Operator::Eq | Operator::NotEq => true,
        Operator::Lt | Operator::LtEq => false,
        Operator::Gt | Operator::GtEq => true,
        _ => {
            panic!("unexpected")
        }
    }
}