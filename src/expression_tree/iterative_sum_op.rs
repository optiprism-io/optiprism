use crate::expression_tree::expr::{Expr};
use std::marker::PhantomData;
use arrow::record_batch::RecordBatch;
use std::ops::{Add, AddAssign};
use crate::expression_tree::boolean_op::BooleanOp;
use arrow::array::{Int8Array, Int64Array};

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

impl<Op> Expr<bool> for IterativeSumOp<i64, Op> where Op: BooleanOp<i64> {
    fn evaluate(&self, batch: &RecordBatch, _: usize) -> bool {
        let arr = batch.columns()[self.left_col_id].as_ref();
        let mut left = 0i64;
        for row_id in 0..arr.len() {
            match self.expr.evaluate(batch, row_id) {
                true => {
                    if !arr.is_null(row_id) {
                        left += arr.as_any().downcast_ref::<Int64Array>().unwrap().value(row_id);
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use arrow::datatypes::{Schema, DataType, Field};
    use arrow::record_batch::RecordBatch;
    use arrow::array::{BooleanArray, Int64Array};
    use crate::expression_tree::iterative_sum_op::IterativeSumOp;
    use datafusion::{
        error::{Result},
    };
    use crate::expression_tree::boolean_op::{Gt, Eq};
    use crate::expression_tree::value_op::ValueOp;
    use crate::expression_tree::expr::Expr;

    #[test]
    fn test() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Boolean, false),
            Field::new("b", DataType::Int64, false),
        ]));

        let a = Arc::new(BooleanArray::from(vec![true, false, true]));
        let b = Arc::new(Int64Array::from(vec![10, 20, 30]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                a.clone(),
                b.clone(),
            ],
        )?;

        let op = IterativeSumOp::<i64, Eq>::new(
            Box::new(ValueOp::<Option<bool>, Eq>::new(0, Some(true))),
            1,
            40,
            false,
        );

        assert_eq!(true, op.evaluate(&batch, 0));
        Ok(())
    }
}