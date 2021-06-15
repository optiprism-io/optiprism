use crate::expression_tree::enum_dispatch::context::Context;
use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::{ColumnarValue, PhysicalExpr};
use std::sync::Arc;
use arrow::array::ArrayRef;
use std::marker::PhantomData;


pub enum ExprEnum<T, Op> {
    BinaryOp { left: ExprEnum<T, _>, op: PhantomData<Op>, right: ExprEnum<_, _> },
    IterativeCountOp { expr: ExprEnum<_, _>, op: PhantomData<Op>, right: i64 },
    Scalar { value: T },
    ValueOp { left_col_id: usize, op: PhantomData<Op>, right: T },
}

pub trait Expr<T> {
    fn evaluate(&self, batch: &RecordBatch, row_id: usize) -> T;
}

impl<T, Op> ExprEnum<T, Op> {
    fn evaluate_bool(&self, batch: &RecordBatch, row_id: usize)->bool {
        match self {
            ExprEnum::BinaryOp { left, op, right } => {
            }
            ExprEnum::IterativeCountOp { expr, op, right } => {}
            ExprEnum::Scalar { value } => {}
            ExprEnum::ValueOp { left_col_id, op, right } => {}
        }
    }
}
