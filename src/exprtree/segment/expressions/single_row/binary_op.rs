use crate::exprtree::segment::expressions::boolean_op::BooleanOp;
use crate::exprtree::segment::expressions::single_row::expr::Expr;
use arrow::array::ArrayRef;
use arrow::record_batch::RecordBatch;
use datafusion::error::Result as DatafusionResult;
use std::marker::PhantomData;
use std::rc::Rc;

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

impl<Op> Expr for BinaryOp<Op>
where
    Op: BooleanOp<bool>,
{
    fn evaluate(&self, batch: &RecordBatch, row_id: usize) -> DatafusionResult<bool> {
        Ok(Op::perform(
            self.left.evaluate(batch, row_id)?,
            self.right.evaluate(batch, row_id)?,
        ))
    }
}

#[cfg(test)]
mod tests {
    use crate::exprtree::segment::expressions::boolean_op::Eq;
    use crate::exprtree::segment::expressions::single_row::binary_op::BinaryOp;
    use crate::exprtree::segment::expressions::single_row::expr::Expr;
    use crate::exprtree::segment::expressions::single_row::test_value::{False, True};
    use arrow::array::{ArrayRef, Int8Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::error::Result;
    use std::sync::Arc;

    #[test]
    fn test() -> Result<()> {
        let op1 = BinaryOp::<Eq>::new(Box::new(True::new()), Box::new(True::new()));

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int8, false)]));
        let a = Arc::new(Int8Array::from(vec![1, 3, 1]));
        let batch = RecordBatch::try_new(schema.clone(), vec![a.clone()])?;
        assert_eq!(true, op1.evaluate(&batch, 0)?);

        let op2 = BinaryOp::<Eq>::new(Box::new(True::new()), Box::new(False::new()));

        assert_eq!(false, op2.evaluate(&batch, 0)?);
        Ok(())
    }
}
