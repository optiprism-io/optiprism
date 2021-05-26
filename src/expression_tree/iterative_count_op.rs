use crate::expression_tree::expr::{Expr};
use std::marker::PhantomData;
use arrow::record_batch::RecordBatch;
use std::ops::{Add, AddAssign};
use crate::expression_tree::boolean_op::BooleanOp;

pub struct IterativeCountOp<Op> {
    expr: Box<dyn Expr<bool>>,
    op: PhantomData<Op>,
    right: i64,
    break_on_false: bool,
}

impl<Op> IterativeCountOp<Op> {
    pub fn new(left: Box<dyn Expr<bool>>, right: i64, break_on_false: bool) -> Self {
        IterativeCountOp {
            expr: left,
            op: PhantomData,
            right,
            break_on_false,
        }
    }
}

impl<Op> Expr<bool> for IterativeCountOp<Op> where Op: BooleanOp<i64> {
    fn evaluate(&self, batch: &RecordBatch, _: usize) -> bool {
        let len = batch.columns()[0].len();
        let mut left: i64 = 0;
        for row_id in 0..len {
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
    use crate::expression_tree::boolean_op::{Gt, Eq, Lt};
    use crate::expression_tree::value_op::ValueOp;
    use crate::expression_tree::expr::Expr;
    use crate::expression_tree::iterative_count_op::IterativeCountOp;

    #[test]
    fn test() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Boolean, false),
        ]));

        let a = Arc::new(BooleanArray::from(vec![true, false, true]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                a.clone(),
            ],
        )?;

        let op1 = IterativeCountOp::<Eq>::new(
            Box::new(ValueOp::<Option<bool>, Eq>::new(0, Some(true))),
            2,
            false,
        );

        assert_eq!(true, op1.evaluate(&batch, 0));

        let op2 = IterativeCountOp::<Lt>::new(
            Box::new(ValueOp::<Option<bool>, Eq>::new(0, Some(true))),
            1,
            false,
        );

        assert_eq!(false, op2.evaluate(&batch, 0));

        let op3 = IterativeCountOp::<Lt>::new(
            Box::new(ValueOp::<Option<bool>, Eq>::new(0, Some(true))),
            1,
            true,
        );

        assert_eq!(false, op3.evaluate(&batch, 0));

        Ok(())
    }
}