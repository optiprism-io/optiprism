use crate::expression_tree::expr::{Expr};
use std::marker::PhantomData;
use crate::expression_tree::context::Context;
use std::rc::Rc;
use arrow::record_batch::RecordBatch;
use crate::expression_tree::boolean_op::BooleanOp;

pub struct BinaryOp<T, Op> {
    left: Box<dyn Expr<T>>,
    op: PhantomData<Op>,
    right: Box<dyn Expr<T>>,
}

impl<T, Op> BinaryOp<T, Op> {
    pub fn new(left: Box<dyn Expr<T>>, right: Box<dyn Expr<T>>) -> Self {
        BinaryOp {
            left,
            op: PhantomData,
            right,
        }
    }
}

impl<T, Op> Expr<bool> for BinaryOp<T, Op> where Op: BooleanOp<T> {
    fn evaluate(&self, batch: &RecordBatch, row_id: usize) -> bool {
        Op::perform(self.left.evaluate(batch, row_id), self.right.evaluate(batch, row_id))
    }
}

#[cfg(test)]
mod tests {
    use datafusion::{
        error::{Result},
    };
    use crate::expression_tree::binary_op::BinaryOp;
    use crate::expression_tree::scalar::Scalar;
    use crate::expression_tree::boolean_op::{Eq};
    use crate::expression_tree::expr::Expr;
    use arrow::record_batch::RecordBatch;
    use arrow::datatypes::Schema;
    use std::sync::Arc;

    #[test]
    fn test() -> Result<()> {
        let b = RecordBatch::new_empty(Arc::new(Schema::empty()));

        let op1 = BinaryOp::<_, Eq>::new(
            Box::new(Scalar::new(1)),
            Box::new(Scalar::new(1)),
        );

        assert_eq!(true, op1.evaluate(&b, 0));

        let op2 = BinaryOp::<_, Eq>::new(
            Box::new(Scalar::new(1)),
            Box::new(Scalar::new(2)),
        );

        assert_eq!(false, op2.evaluate(&b, 0));
        Ok(())
    }
}