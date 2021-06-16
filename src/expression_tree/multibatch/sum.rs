use std::sync::Arc;
use datafusion::physical_plan::{PhysicalExpr, ColumnarValue};
use crate::error::{Result};
use arrow::record_batch::RecordBatch;
use arrow::array::{ArrayRef, BooleanArray, Int8Array, Array};
use crate::expression_tree::multibatch::expr::Expr;
use arrow::compute::kernels::arithmetic::{
    add, divide, divide_scalar, multiply, subtract,
};
use std::ops::{Add, AddAssign};
use crate::expression_tree::utils::{into_array, break_on_true, break_on_false};
use std::marker::PhantomData;
use datafusion::error::{DataFusionError, Result as DatafusionResult};
use arrow::datatypes::{DataType, Schema};
use crate::expression_tree::boolean_op::BooleanOp;

pub struct Sum<L, R, Op> {
    predicate: Arc<dyn PhysicalExpr>,
    lt: PhantomData<L>,
    left_col_id: usize,
    op: PhantomData<Op>,
    right: R,
}

impl<L, R, Op> Sum<L, R, Op> {
    pub fn try_new(schema: &Schema, left_col: &str, predicate: Arc<dyn PhysicalExpr>, right: R) -> DatafusionResult<Self> {
        let (left_col_id, left_field) = schema.column_with_name(left_col).ok_or_else(|| DataFusionError::Plan(format!("Column {} not found", left_col)))?;
        match left_field.data_type() {
            DataType::Int8 => {}
            other => return Err(DataFusionError::Plan(format!(
                "Left column must be summable, not {:?}",
                other,
            )))
        };

        match predicate.data_type(schema)? {
            DataType::Boolean => Ok(Sum {
                predicate,
                lt: PhantomData,
                left_col_id,
                op: PhantomData,
                right,
            }),
            other => Err(DataFusionError::Plan(format!(
                "Sum predicate must return boolean values, not {:?}",
                other,
            )))
        }
    }
}


impl<Op> Expr for Sum<i8, i64, Op> where Op: BooleanOp<i64> {
    fn evaluate(&self, batches: &[&RecordBatch]) -> DatafusionResult<bool> {
        let mut acc: i64 = 0;

        for batch in batches.iter() {
            let ar = into_array(self.predicate.evaluate(batch).unwrap());
            let b = ar.as_any().downcast_ref::<BooleanArray>().ok_or_else(|| DataFusionError::Internal("failed to downcast to BooleanArray".to_string()))?;
            let v = batch.columns()[self.left_col_id].as_any().downcast_ref::<Int8Array>().ok_or_else(|| DataFusionError::Internal("failed to downcast to Int8Array".to_string()))?;
            acc += b
                .iter()
                .enumerate()
                .filter(|(i, x)| x.is_some() && x.unwrap() && !v.data_ref().is_null(*i))
                .map(|(i, _)| v.value(i))
                .fold(0i64, |a, x| a + x as i64);
            let res = Op::perform(acc, self.right);
            if res && break_on_true(Op::op()) {
                return Ok(true);
            } else if !res && break_on_false(Op::op()) {
                return Ok(false);
            }
        }

        Ok(Op::perform(acc, self.right))
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
    use crate::expression_tree::multibatch::count::Count;
    use datafusion::physical_plan::expressions::{BinaryExpr, Column, Literal};
    use datafusion::logical_plan::Operator;
    use datafusion::scalar::ScalarValue;
    use crate::expression_tree::multibatch::expr::Expr;
    use crate::expression_tree::multibatch::sum::Sum;
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
        let op = Sum::<i8, i64, Eq>::try_new(
            &schema,
            "b",
            bo.clone(),
            254,
        )?;

        assert_eq!(true, op.evaluate(vec![&batch].as_slice())?);
        Ok(())
    }
}