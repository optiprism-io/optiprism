use std::sync::Arc;
use datafusion::physical_plan::{PhysicalExpr, ColumnarValue};
use crate::error::{Result};
use arrow::record_batch::RecordBatch;
use arrow::array::{ArrayRef, BooleanArray};
use crate::expression_tree::expr::{Expr};
use arrow::compute::kernels::arithmetic::{
    add, divide, divide_scalar, multiply, subtract,
};
use crate::expression_tree::utils::into_array;
use std::marker::PhantomData;
use crate::expression_tree::boolean_op::BooleanOp;
use crate::expression_tree::utils::{break_on_true, break_on_false};

pub struct Count<Op> {
    expr: Arc<dyn PhysicalExpr>,
    op: PhantomData<Op>,
    right: i64,
}

impl<Op> Count<Op> {
    pub fn new(expr: Arc<dyn PhysicalExpr>, right: i64) -> Self {
        Count {
            expr,
            op: PhantomData,
            right,
        }
    }
}

impl<Op> Expr for Count<Op> where Op: BooleanOp<i64> {
    fn evaluate(&self, batches:  &[&RecordBatch]) -> bool {
        let mut acc: i64 = 0;

        for batch in batches.iter() {
            let ar = into_array(self.expr.evaluate(batch).unwrap());
            let b = ar.as_any().downcast_ref::<BooleanArray>().unwrap();
            acc += b
                .iter()
                .filter(|x| x.is_some() && x.unwrap())
                .count() as i64;
            let res = Op::perform(acc, self.right);
            if res && break_on_true(Op::op()) {
                return true;
            } else if !res && break_on_false(Op::op()) {
                return false;
            }
        }

        Op::perform(acc, self.right)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use arrow::datatypes::{Schema, DataType, Field};
    use arrow::record_batch::RecordBatch;
    use arrow::array::{ArrayRef, BooleanArray, Int8Array};
    use datafusion::{
        error::{Result},
    };
    use crate::expression_tree::count::Count;
    use datafusion::physical_plan::expressions::{BinaryExpr, Column, Literal};
    use datafusion::logical_plan::Operator;
    use datafusion::scalar::ScalarValue;
    use crate::expression_tree::expr::Expr;
    use crate::expression_tree::boolean_op::{Eq, Gt, Lt};

    #[test]
    fn test() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int8, false),
        ]));

        let a = Arc::new(Int8Array::from(vec![1, 3, 1]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                a.clone(),
            ],
        )?;

        let left = Column::new("a");
        let right = Literal::new(ScalarValue::Int8(Some(1)));
        let bo = Arc::new(BinaryExpr::new(Arc::new(left), Operator::Eq, Arc::new(right)));

        let op1 = Count::<Eq>::new(
            bo.clone(),
            2,
        );

        assert_eq!(true, op1.evaluate(vec![&batch].as_slice()));

        let op2 = Count::<Lt>::new(
            bo.clone(),
            1,
        );

        assert_eq!(false, op2.evaluate(vec![&batch].as_slice()));

        let op3 = Count::<Lt>::new(
            bo.clone(),
            1,
        );

        assert_eq!(false, op3.evaluate(vec![&batch].as_slice()));

        let op4 = Count::<Gt>::new(
            bo.clone(),
            1,
        );

        assert_eq!(true, op4.evaluate(vec![&batch].as_slice()));

        Ok(())
    }
}