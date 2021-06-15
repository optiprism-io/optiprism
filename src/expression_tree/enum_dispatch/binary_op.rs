use crate::expression_tree::enum_dispatch::expr::{Expr, ExprEnum};
use std::marker::PhantomData;
use crate::expression_tree::enum_dispatch::context::Context;
use std::rc::Rc;
use arrow::record_batch::RecordBatch;
use crate::expression_tree::enum_dispatch::boolean_op::BooleanOp;

pub struct BinaryOp<T, Op> {
    left: ExprEnum,
    op: PhantomData<Op>,
    right: ExprEnum,
}

impl<T, Op> BinaryOp<T, Op> {
    pub fn new(left: ExprEnum, right: ExprEnum) -> Self {
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
    use crate::expression_tree::enum_dispatch::binary_op::BinaryOp;
    use crate::expression_tree::enum_dispatch::scalar::Scalar;
    use crate::expression_tree::enum_dispatch::boolean_op::{Eq};
    use crate::expression_tree::enum_dispatch::expr::Expr;
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