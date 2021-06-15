use crate::expression_tree::multibatch::expr::{Expr};
use std::marker::PhantomData;
use std::rc::Rc;
use arrow::record_batch::RecordBatch;
use crate::expression_tree::multibatch::boolean_op::BooleanOp;
use arrow::array::ArrayRef;
use datafusion::error::{Result as DatafusionResult};

pub struct BinaryOp<Op> {
    left: Box<dyn Expr>,
    op: PhantomData<Op>,
    right: Box<dyn Expr>,
}

impl<Op> BinaryOp<Op> {
    pub fn new(left: Box<dyn Expr>, right: Box<dyn Expr>) -> Self {
        BinaryOp {
            left,
            op: PhantomData,
            right,
        }
    }
}

impl<Op> Expr for BinaryOp<Op> where Op: BooleanOp<bool> {
    fn evaluate(&self, batches:  &[&RecordBatch]) -> DatafusionResult<bool> {
        Ok(Op::perform(self.left.evaluate(batches)?, self.right.evaluate(batches)?))
    }
}

#[cfg(test)]
mod tests {
    use datafusion::{
        error::{Result},
    };
    use crate::expression_tree::multibatch::binary_op::BinaryOp;
    use crate::expression_tree::multibatch::boolean_op::{Eq};
    use crate::expression_tree::multibatch::expr::Expr;
    use crate::expression_tree::multibatch::test_value::{True, False};
    use arrow::record_batch::RecordBatch;
    use arrow::datatypes::{Schema, Field, DataType};
    use std::sync::Arc;
    use arrow::array::{ArrayRef, Int8Array};

    #[test]
    fn test() -> Result<()> {
        let op1 = BinaryOp::<Eq>::new(
            Box::new(True::new()),
            Box::new(True::new()),
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
        assert_eq!(true, op1.evaluate(vec![&batch].as_slice())?);

        let op2 = BinaryOp::<Eq>::new(
            Box::new(True::new()),
            Box::new(False::new()),
        );

        assert_eq!(false, op2.evaluate(vec![&batch].as_slice())?);
        Ok(())
    }
}