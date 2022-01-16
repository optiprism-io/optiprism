use crate::exprtree::segment::expressions::boolean_op::BooleanOp;
use crate::exprtree::segment::expressions::expr::Expr;
use arrow::record_batch::RecordBatch;
use datafusion::logical_plan::Operator;
use std::marker::PhantomData;
use std::ops::{Add, AddAssign};

pub struct IterativeCountOp<Op> {
    expr: Box<dyn Expr<bool>>,
    op: PhantomData<Op>,
    right: i64,
}

impl<Op> IterativeCountOp<Op> {
    pub fn new(left: Box<dyn Expr<bool>>, right: i64) -> Self {
        IterativeCountOp {
            expr: left,
            op: PhantomData,
            right,
        }
    }
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

impl<Op> Expr<bool> for IterativeCountOp<Op>
where
    Op: BooleanOp<i64>,
{
    fn evaluate(&self, batch: &RecordBatch, _: usize) -> bool {
        let mut left: i64 = 0;
        let break_on_false = break_on_false(Op::op());
        let break_on_true = break_on_true(Op::op());

        for row_id in 0..batch.num_rows() {
            match self.expr.evaluate(batch, row_id) {
                true => {
                    left += 1;
                    let res = Op::perform(left, self.right);

                    if res && break_on_true {
                        return true;
                    } else if !res && break_on_false {
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
    use crate::exprtree::segment::expressions::boolean_op::{Eq, Gt, Lt};
    use crate::exprtree::segment::expressions::expr::Expr;
    use crate::exprtree::segment::expressions::iterative_count_op::IterativeCountOp;
    use crate::exprtree::segment::expressions::iterative_sum_op::IterativeSumOp;
    use crate::exprtree::segment::expressions::value_op::ValueOp;
    use arrow::array::{BooleanArray, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::error::Result;
    use std::sync::Arc;

    #[test]
    fn test() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Boolean, false)]));

        let a = Arc::new(BooleanArray::from(vec![true, false, true]));
        let batch = RecordBatch::try_new(schema.clone(), vec![a.clone()])?;

        let op1 = IterativeCountOp::<Eq>::new(
            Box::new(ValueOp::<Option<bool>, Eq>::new(0, Some(true))),
            2,
        );

        assert_eq!(true, op1.evaluate(&batch, 0));

        let op2 = IterativeCountOp::<Lt>::new(
            Box::new(ValueOp::<Option<bool>, Eq>::new(0, Some(true))),
            1,
        );

        assert_eq!(false, op2.evaluate(&batch, 0));

        let op3 = IterativeCountOp::<Lt>::new(
            Box::new(ValueOp::<Option<bool>, Eq>::new(0, Some(true))),
            1,
        );

        assert_eq!(false, op3.evaluate(&batch, 0));

        let op4 = IterativeCountOp::<Gt>::new(
            Box::new(ValueOp::<Option<bool>, Eq>::new(0, Some(true))),
            1,
        );

        assert_eq!(true, op4.evaluate(&batch, 0));

        Ok(())
    }
}
