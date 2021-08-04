use crate::segment::expressions::multibatch::expr::{Expr};
use std::marker::PhantomData;
use std::rc::Rc;
use arrow::record_batch::RecordBatch;
use arrow::array::ArrayRef;
use datafusion::error::{Result as DatafusionResult};
use crate::segment::expressions::boolean_op::BooleanOp;
use std::sync::Arc;

#[derive(Debug)]
pub struct BinaryOp<Op> {
    left: Arc<dyn Expr>,
    op: PhantomData<Op>,
    right: Arc<dyn Expr>,
}

impl<Op> BinaryOp<Op> {
    pub fn new(left: Arc<dyn Expr>, right: Arc<dyn Expr>) -> Self {
        BinaryOp {
            left,
            op: PhantomData,
            right,
        }
    }
}

impl<Op> Expr for BinaryOp<Op> where Op: BooleanOp<bool> {
    fn evaluate(&self, batches: &[RecordBatch]) -> DatafusionResult<bool> {
        Ok(Op::perform(self.left.evaluate(batches)?, self.right.evaluate(batches)?))
    }
}

impl<Op: BooleanOp<bool>> std::fmt::Display for BinaryOp<Op> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", "BinaryOp")
    }
}

#[cfg(test)]
mod tests {
    use datafusion::{
        error::{Result},
    };
    use crate::segment::expressions::multibatch::binary_op::BinaryOp;
    use crate::segment::expressions::boolean_op::{Eq};
    use crate::segment::expressions::multibatch::expr::Expr;
    use crate::segment::expressions::multibatch::test_value::{True, False};
    use arrow::record_batch::RecordBatch;
    use arrow::datatypes::{Schema, Field, DataType};
    use std::sync::Arc;
    use arrow::array::{ArrayRef, Int8Array};

    #[test]
    fn test() -> Result<()> {
        let op1 = BinaryOp::<Eq>::new(
            Arc::new(True::new()),
            Arc::new(True::new()),
        );

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
        assert_eq!(true, op1.evaluate(vec![batch.clone()].as_slice())?);

        let op2 = BinaryOp::<Eq>::new(
            Arc::new(True::new()),
            Arc::new(False::new()),
        );

        assert_eq!(false, op2.evaluate(vec![batch.clone()].as_slice())?);
        Ok(())
    }
}