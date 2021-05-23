use crate::expression_tree::expr::{Expr};
use std::marker::PhantomData;
use arrow::record_batch::RecordBatch;
use std::ops::{Add, AddAssign};
use crate::expression_tree::boolean_op::BooleanOp;

pub struct IterativeCountOp<Op> {
    expr: Box<dyn Expr<bool>>,
    op: PhantomData<Op>,
    right: usize,
    break_on_false: bool,
}

impl<Op> IterativeCountOp<Op> {
    pub fn new(left: Box<dyn Expr<bool>>, right: usize, break_on_false: bool) -> Self {
        IterativeCountOp {
            expr: left,
            op: PhantomData,
            right,
            break_on_false,
        }
    }
}

impl<Op> Expr<bool> for IterativeCountOp<Op> where Op: BooleanOp<usize> {
    fn evaluate(&self, batch: &RecordBatch, _: usize) -> bool {
        let len = batch.columns()[0].len();
        let mut left: usize = 0;
        for row_id in 0..len - 1 {
            match self.expr.evaluate(batch, row_id) {
                true => {
                    left += 1;
                    if !Op::perform(left, self.right) && self.break_on_false {
                        return false;
                    }
                }
                false => {}
            }
        }

        return Op::perform(left, self.right);
    }
}