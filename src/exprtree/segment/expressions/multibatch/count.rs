use crate::exprtree::error::Result;
use crate::exprtree::segment::expressions::boolean_op::BooleanOp;
use crate::exprtree::segment::expressions::multibatch::expr::Expr;
use crate::exprtree::segment::expressions::utils::into_array;
use crate::exprtree::segment::expressions::utils::{break_on_false, break_on_true};
use arrow::array::{ArrayRef, BooleanArray};
use arrow::compute::kernels::arithmetic::{add, divide, divide_scalar, multiply, subtract};
use arrow::datatypes::{DataType, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DatafusionResult};
use datafusion::physical_plan::{ColumnarValue, PhysicalExpr};
use std::marker::PhantomData;
use std::sync::Arc;

#[derive(Debug)]
pub struct Count<Op> {
    predicate: Arc<dyn PhysicalExpr>,
    op: PhantomData<Op>,
    right: i64,
}

impl<Op> Count<Op> {
    pub fn try_new(
        schema: &Schema,
        predicate: Arc<dyn PhysicalExpr>,
        right: i64,
    ) -> DatafusionResult<Self> {
        match predicate.data_type(schema)? {
            DataType::Boolean => Ok(Count {
                predicate,
                op: PhantomData,
                right,
            }),
            other => Err(DataFusionError::Plan(format!(
                "Count predicate must return boolean values, not {:?}",
                other,
            ))),
        }
    }
}

impl<Op> Expr for Count<Op>
where
    Op: BooleanOp<i64>,
{
    fn evaluate(&self, batches: &[RecordBatch]) -> DatafusionResult<bool> {
        let mut acc: i64 = 0;

        for batch in batches.iter() {
            let ar = into_array(self.predicate.evaluate(batch)?);
            let b = ar.as_any().downcast_ref::<BooleanArray>().ok_or_else(|| {
                DataFusionError::Internal("failed to downcast to BooleanArray".to_string())
            })?;
            acc += b.iter().filter(|x| x.is_some() && x.unwrap()).count() as i64;
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

impl<Op: BooleanOp<i64>> std::fmt::Display for Count<Op> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", "Count")
    }
}

#[cfg(test)]
mod tests {
    use crate::exprtree::segment::expressions::boolean_op::{Eq, Gt, Lt};
    use crate::exprtree::segment::expressions::multibatch::count::Count;
    use crate::exprtree::segment::expressions::multibatch::expr::Expr;
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
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int8, false)]));

        let a = Arc::new(Int8Array::from(vec![1, 3, 1]));
        let batch = RecordBatch::try_new(schema.clone(), vec![a.clone()])?;

        let left = Column::new_with_schema("a", &schema)?;
        let right = Literal::new(ScalarValue::Int8(Some(1)));
        let bo = Arc::new(BinaryExpr::new(
            Arc::new(left),
            Operator::Eq,
            Arc::new(right),
        ));

        let op1 = Count::<Eq>::try_new(&schema, bo.clone(), 2)?;

        assert_eq!(true, op1.evaluate(vec![batch.clone()].as_slice())?);

        let op2 = Count::<Lt>::try_new(&schema, bo.clone(), 1)?;

        assert_eq!(false, op2.evaluate(vec![batch.clone()].as_slice())?);

        let op3 = Count::<Lt>::try_new(&schema, bo.clone(), 1)?;

        assert_eq!(false, op3.evaluate(vec![batch.clone()].as_slice())?);

        let op4 = Count::<Gt>::try_new(&schema, bo.clone(), 1)?;

        assert_eq!(true, op4.evaluate(vec![batch.clone()].as_slice())?);

        Ok(())
    }
}
