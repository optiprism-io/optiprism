use crate::exprtree::error::Result;
use crate::exprtree::segment::expressions::boolean_op::BooleanOp;
use crate::exprtree::segment::expressions::multibatch::expr::Expr;
use crate::exprtree::segment::expressions::utils::{break_on_false, break_on_true, into_array};
use arrow::array::{Array, ArrayRef, BooleanArray, Int8Array};
use arrow::compute::kernels::arithmetic::{add, divide, divide_scalar, multiply, subtract};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DatafusionResult};
use datafusion::logical_plan::ToDFSchema;
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::{ColumnarValue, PhysicalExpr};
use std::marker::PhantomData;
use std::ops::{Add, AddAssign};
use std::sync::Arc;

#[derive(Debug)]
pub struct Sum<L, R, Op> {
    predicate: Arc<dyn PhysicalExpr>,
    lt: PhantomData<L>,
    left_col: Column,
    op: PhantomData<Op>,
    right: R,
}

impl<L, R, Op> Sum<L, R, Op> {
    pub fn try_new(
        schema: &Schema,
        left_col: Column,
        predicate: Arc<dyn PhysicalExpr>,
        right: R,
    ) -> DatafusionResult<Self> {
        let (left_field) = schema.field_with_name(left_col.name())?;
        match left_field.data_type() {
            DataType::Int8 => {}
            other => {
                return Err(DataFusionError::Plan(format!(
                    "Left column must be summable, not {:?}",
                    other,
                )))
            }
        };

        match predicate.data_type(schema)? {
            DataType::Boolean => Ok(Sum {
                predicate,
                lt: PhantomData,
                left_col: left_col.clone(),
                op: PhantomData,
                right,
            }),
            other => Err(DataFusionError::Plan(format!(
                "Sum predicate must return boolean values, not {:?}",
                other,
            ))),
        }
    }
}

impl<Op> Expr for Sum<i8, i64, Op>
where
    Op: BooleanOp<i64>,
{
    fn evaluate(&self, batches: &[RecordBatch]) -> DatafusionResult<bool> {
        let mut acc: i64 = 0;

        for batch in batches.iter() {
            let ar = into_array(self.predicate.evaluate(batch).unwrap());
            let b = ar.as_any().downcast_ref::<BooleanArray>().ok_or_else(|| {
                DataFusionError::Internal("failed to downcast to BooleanArray".to_string())
            })?;
            let v = into_array(
                self.left_col
                    .evaluate(&batch)
                    .or_else(|e| Err(e.into_arrow_external_error()))?,
            );
            let v = v.as_any().downcast_ref::<Int8Array>().ok_or_else(|| {
                DataFusionError::Internal("failed to downcast to Int8Array".to_string())
            })?;
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

impl<Op: BooleanOp<i64>> std::fmt::Display for Sum<i8, i64, Op> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", "Sum")
    }
}

#[cfg(test)]
mod tests {
    use crate::exprtree::segment::expressions::boolean_op::{Eq, Gt, Lt};
    use crate::exprtree::segment::expressions::multibatch::count::Count;
    use crate::exprtree::segment::expressions::multibatch::expr::Expr;
    use crate::exprtree::segment::expressions::multibatch::sum::Sum;
    use arrow::array::{ArrayRef, BooleanArray, Int8Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::error::Result;
    use datafusion::logical_plan::Operator;
    use datafusion::physical_plan::expressions::{BinaryExpr, Column, Literal};
    use datafusion::scalar::ScalarValue;
    use std::sync::Arc;

    #[test]
    fn test() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int8, false),
            Field::new("b", DataType::Int8, false),
        ]));

        let a = Arc::new(Int8Array::from(vec![1, 2, 1]));
        let b = Arc::new(Int8Array::from(vec![127, 100, 127]));
        let batch = RecordBatch::try_new(schema.clone(), vec![a.clone(), b.clone()])?;

        let left = Column::new_with_schema("a", &schema).unwrap();
        let right = Literal::new(ScalarValue::Int8(Some(1)));
        let bo = Arc::new(BinaryExpr::new(
            Arc::new(left),
            Operator::Eq,
            Arc::new(right),
        ));
        let op = Sum::<i8, i64, Eq>::try_new(
            &schema,
            Column::new_with_schema("b", &schema)?,
            bo.clone(),
            254,
        )?;

        assert_eq!(true, op.evaluate(vec![batch].as_slice())?);
        Ok(())
    }
}
