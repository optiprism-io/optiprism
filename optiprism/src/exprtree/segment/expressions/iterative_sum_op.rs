use crate::exprtree::segment::expressions::boolean_op::BooleanOp;
use crate::exprtree::segment::expressions::expr::Expr;
use crate::exprtree::segment::expressions::iterative_count_op::{break_on_false, break_on_true};
use arrow::array::{Int64Array, Int8Array};
use arrow::record_batch::RecordBatch;
use std::marker::PhantomData;
use std::ops::{Add, AddAssign};

pub struct IterativeSumOp<T, Op> {
    expr: Box<dyn Expr<bool>>,
    left_col_id: usize,
    op: PhantomData<Op>,
    right: T,
}

impl<T, Op> IterativeSumOp<T, Op> {
    pub fn new(expr: Box<dyn Expr<bool>>, left_col_id: usize, right: T) -> Self {
        IterativeSumOp {
            expr,
            left_col_id,
            op: PhantomData,
            right,
        }
    }
}

impl<Op> Expr<bool> for IterativeSumOp<i64, Op>
where
    Op: BooleanOp<i64>,
{
    fn evaluate(&self, batch: &RecordBatch, _: usize) -> bool {
        let arr = batch.columns()[self.left_col_id].as_ref();
        let mut left = 0i64;
        let break_on_false = break_on_false(Op::op());
        let break_on_true = break_on_true(Op::op());

        for row_id in 0..arr.len() {
            match self.expr.evaluate(batch, row_id) {
                true => {
                    if !arr.is_null(row_id) {
                        left += arr
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap()
                            .value(row_id);
                        let res = Op::perform(left, self.right);

                        if res && break_on_true {
                            return true;
                        } else if !res && break_on_false {
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
    use crate::exprtree::segment::expressions::boolean_op::{Eq, Gt};
    use crate::exprtree::segment::expressions::expr::Expr;
    use crate::exprtree::segment::expressions::iterative_sum_op::IterativeSumOp;
    use crate::exprtree::segment::expressions::value_op::ValueOp;
    use arrow::array::{BooleanArray, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::error::Result;
    use std::sync::Arc;

    #[test]
    fn test() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Boolean, false),
            Field::new("b", DataType::Int64, false),
        ]));

        let a = Arc::new(BooleanArray::from(vec![true, false, true]));
        let b = Arc::new(Int64Array::from(vec![10, 20, 30]));
        let batch = RecordBatch::try_new(schema.clone(), vec![a.clone(), b.clone()])?;

        let op = IterativeSumOp::<i64, Eq>::new(
            Box::new(ValueOp::<Option<bool>, Eq>::new(0, Some(true))),
            1,
            40,
        );

        assert_eq!(true, op.evaluate(&batch, 0));
        Ok(())
    }
}
