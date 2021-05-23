use crate::expression_tree::expr::{Expr};
use std::marker::PhantomData;
use arrow::record_batch::RecordBatch;
use std::ops::{Add, AddAssign};
use crate::expression_tree::boolean_op::BooleanOp;
use arrow::array::Int8Array;

pub struct IterativeSumOp<T, Op> {
    expr: Box<dyn Expr<bool>>,
    left_col_id: usize,
    op: PhantomData<Op>,
    right: T,
    break_on_false: bool,
}

impl<T, Op> IterativeSumOp<T, Op> {
    pub fn new(expr: Box<dyn Expr<bool>>, left_col_id: usize, right: T, break_on_false: bool) -> Self {
        IterativeSumOp {
            expr,
            left_col_id,
            op: PhantomData,
            right,
            break_on_false,
        }
    }
}

impl<Op> Expr<bool> for IterativeSumOp<i8, Op> where Op: BooleanOp<i8> {
    fn evaluate(&self, batch: &RecordBatch, _: usize) -> bool {
        let arr = batch.columns()[self.left_col_id].as_ref();
        let mut left = 0i8;
        for row_id in 0..arr.len() - 1 {
            match self.expr.evaluate(batch, row_id) {
                true => {
                    if !arr.is_null(row_id) {
                        left += arr.as_any().downcast_ref::<Int8Array>().unwrap().value(row_id);
                        if !Op::perform(left, self.right) && self.break_on_false {
                            return false;
                        }
                    }
                }
                false => {}
            }
        }

        return Op::perform(left, self.right);
    }
}