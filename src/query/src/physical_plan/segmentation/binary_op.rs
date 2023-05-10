use arrow::array::ArrayRef;
use arrow::record_batch::RecordBatch;
use std::rc::Rc;
use std::sync::Arc;
use datafusion_expr::{binary_expr, Operator};
use crate::physical_plan::segment::Expr;
use crate::error::Result;

#[derive(Debug)]
pub struct BinaryOp {
    left: Arc<dyn Expr>,
    op: Operator,
    right: Arc<dyn Expr>,
}

impl BinaryOp {
    pub fn new(left: Arc<dyn Expr>, op: Operator, right: Arc<dyn Expr>) -> Self {
        BinaryOp {
            left,
            op,
            right,
        }
    }
}

impl Expr for BinaryOp
{
    fn evaluate(&self, batch: &RecordBatch) -> Result<bool> {
        let left = self.left.evaluate(batch)?;
        let right = self.right.evaluate(batch)?;
        let res = match self.op {
            Operator::Eq => left == right,
            Operator::NotEq => left != right,
            Operator::Lt => left < right,
            Operator::LtEq => left <= right,
            Operator::Gt => left > right,
            Operator::GtEq => left >= right,
            Operator::And => left && right,
            Operator::Or => left || right,
            _ => unreachable!("unexpected operator {}", self.op)
        };

        Ok(res)
    }
}

impl std::fmt::Display for BinaryOp {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}{}{}", self.left, self.op, self.right)
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{ArrayRef, Int8Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;
    use datafusion_expr::Operator;
    use crate::physical_plan::segment::Expr;
    use crate::error::Result;
    use crate::physical_plan::segment::binary_op::BinaryOp;

    macro_rules! mock_expr {
        ($op:expr) => {{
            #[derive(Debug)]
            struct MockExpr;

            impl Expr for MockExpr {
                fn evaluate(&self, _: &RecordBatch) -> Result<bool> {
                    Ok($op)
                }
            }

            impl std::fmt::Display for MockExpr {
                fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                    write!(f, "{}", "$op")
                }
            }

            Arc::new(MockExpr{})
        }};
    }


    #[test]
    fn it_works() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int8, false)]));
        let batch = RecordBatch::new_empty(schema);
        let op = BinaryOp::new(mock_expr!(true), Operator::Eq, mock_expr!(true));
        assert_eq!(true, op.evaluate(&batch)?);

        let op = BinaryOp::new(mock_expr!(true), Operator::Eq, mock_expr!(false));
        assert_eq!(false, op.evaluate(&batch)?);

        let op = BinaryOp::new(mock_expr!(true), Operator::And, mock_expr!(true));
        assert_eq!(true, op.evaluate(&batch)?);

        Ok(())
    }
}
