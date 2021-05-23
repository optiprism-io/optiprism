use crate::expression_tree::expr::{Expr};
use std::marker::PhantomData;
use crate::expression_tree::context::Context;
use std::rc::Rc;
use arrow::record_batch::RecordBatch;
use crate::expression_tree::boolean_op::BooleanOp;

pub struct BinaryOp<T, Op> {
    left: Box<dyn Expr<T>>,
    op: PhantomData<Op>,
    right: Box<dyn Expr<T>>,
}

impl<T, Op> BinaryOp<T, Op> {
    pub fn new(left: Box<dyn Expr<T>>, right: Box<dyn Expr<T>>) -> Self {
        BinaryOp {
            left,
            op: PhantomData,
            right,
        }
    }
}

impl<T, Op> Expr<bool> for BinaryOp<T, Op> where Op: BooleanOp<T> {
    fn evaluate(&self, batch: &RecordBatch, row_id: usize) -> bool {
        Op::perform(self.left.evaluate(batch, row_id), self.right.evaluate(batch, row_id))
    }
}