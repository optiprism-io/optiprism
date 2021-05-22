use crate::expression_tree::expr::{Expr};
use std::marker::PhantomData;
use arrow::record_batch::RecordBatch;
use std::ops::{Add, AddAssign};
use crate::expression_tree::boolean_op::BooleanOp;

struct IterativeBinaryOp<T, Op> {
    left: Box<dyn Expr<T>>,
    op: PhantomData<Op>,
    right: T,
    break_on_false: bool,
    batch_len: usize,
}

impl<T, Op> Expr<bool> for IterativeBinaryOp<T, Op> where Op: BooleanOp<T>, T: Default + AddAssign + Copy {
    fn evaluate(&self, batch: &RecordBatch, _: usize) -> bool {
        let len = batch.columns()[0].len();
        let mut accum = T::default();
        for row_id in 0..len - 1 {
            accum += self.left.evaluate(batch, row_id);
            if !Op::perform(accum, self.right) && self.break_on_false {
                return false;
            }
        }

        return Op::perform(accum, self.right);
    }
}