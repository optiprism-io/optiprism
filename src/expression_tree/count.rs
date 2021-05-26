use std::sync::Arc;
use datafusion::physical_plan::{PhysicalExpr, ColumnarValue};
use crate::error::{Result};
use arrow::record_batch::RecordBatch;
use arrow::array::{ArrayRef, BooleanArray};
use crate::expression_tree::expr::Expr;
use arrow::compute::kernels::arithmetic::{
    add, divide, divide_scalar, multiply, subtract,
};

pub struct Count {
    predicate: Arc<dyn PhysicalExpr>,
}

impl Count {
    pub fn new(predicate: Arc<dyn PhysicalExpr>) -> Self {
        Count {
            predicate,
        }
    }
}

impl Expr<i64> for Count {
    fn evaluate(&self, batch: &RecordBatch, row_id: usize) -> i64 {
        let a =
        if let ColumnarValue::Array(ar) = self.predicate.evaluate(batch).unwrap()
        {
            let mut acc = 0i64;
            let ba = ar.as_any().downcast_ref::<BooleanArray>().unwrap();
            for i in ba.iter() {
                match i {
                    Some(v) => {
                        if v {
                            acc += 1;
                        }
                    }
                    None => {}
                }
            }
            return acc;
        };
        // todo: return result
        panic!("unexpected columnar value");
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

    #[test]
    fn test() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int8, false),
        ]));

        let a = Arc::new(Int8Array::from(vec![1, 0, 1]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                a.clone(),
            ],
        )?;

        let left = Column::new("a");
        let right = Literal::new(ScalarValue::Int8(Some(1)));
        let bo = BinaryExpr::new(Arc::new(left), Operator::Eq, Arc::new(right));
        let c = Count::new(Arc::new(bo));

        assert_eq!(2, c.evaluate(&batch, 0));
        Ok(())
    }
}