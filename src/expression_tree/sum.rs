use std::sync::Arc;
use datafusion::physical_plan::{PhysicalExpr, ColumnarValue};
use crate::error::{Result};
use arrow::record_batch::RecordBatch;
use arrow::array::{ArrayRef, BooleanArray, Int8Array, Array};
use crate::expression_tree::expr::Expr;
use arrow::compute::kernels::arithmetic::{
    add, divide, divide_scalar, multiply, subtract,
};
use std::ops::{Add, AddAssign};
use crate::expression_tree::utils::{into_array, break_on_true, break_on_false};
use std::marker::PhantomData;
use crate::expression_tree::boolean_op::BooleanOp;

pub struct Sum<L, R, Op> {
    expr: Arc<dyn PhysicalExpr>,
    lt: PhantomData<L>,
    left_col_id: usize,
    op: PhantomData<Op>,
    right: R,
}

impl<L, R, Op> Sum<L, R, Op> {
    pub fn new(left_col_id: usize, expr: Arc<dyn PhysicalExpr>, right: R) -> Self {
        Sum {
            expr,
            lt: PhantomData,
            left_col_id,
            op: PhantomData,
            right,
        }
    }
}


impl<Op> Expr for Sum<i8, i64, Op> where Op: BooleanOp<i64> {
    fn evaluate(&self, batches: &[&RecordBatch]) -> bool {
        let mut acc: i64 = 0;

        for batch in batches.iter() {
            let ar = into_array(self.expr.evaluate(batch).unwrap());
            let b = ar.as_any().downcast_ref::<BooleanArray>().unwrap();
            let v = batch.columns()[self.left_col_id].as_any().downcast_ref::<Int8Array>().unwrap();
            acc += b
                .iter()
                .enumerate()
                .filter(|(i, x)| x.is_some() && x.unwrap() && !v.data_ref().is_null(*i))
                .map(|(i, _)| v.value(i))
                .fold(0i64, |a, x| a + x as i64);
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
    use crate::expression_tree::sum::Sum;
    use crate::expression_tree::boolean_op::{Eq, Gt, Lt};

    #[test]
    fn test() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int8, false),
            Field::new("b", DataType::Int8, false),
        ]));

        let a = Arc::new(Int8Array::from(vec![1, 2, 1]));
        let b = Arc::new(Int8Array::from(vec![127, 100, 127]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                a.clone(),
                b.clone(),
            ],
        )?;

        let left = Column::new("a");
        let right = Literal::new(ScalarValue::Int8(Some(1)));
        let bo = Arc::new(BinaryExpr::new(Arc::new(left), Operator::Eq, Arc::new(right)));
        let op = Sum::<i8, i64, Eq>::new(
            1,
            bo.clone(),
            254,
        );

        assert_eq!(true, op.evaluate(vec![&batch].as_slice()));
        Ok(())
    }
}