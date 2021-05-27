use crate::expression_tree::expr::Expr;
use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::{PhysicalExpr, ColumnarValue};
use std::sync::Arc;
use arrow::array::{BooleanArray, Array, ArrayRef};
use std::borrow::Borrow;

pub struct Sequence {
    steps: Vec<Arc<dyn PhysicalExpr>>,
}

impl Sequence {
    pub fn new(steps: Vec<Arc<dyn PhysicalExpr>>) -> Self {
        Sequence {
            steps,
        }
    }
}

impl Expr<bool> for Sequence {
    fn evaluate(&self, batch: &RecordBatch, _: usize) -> bool {
        let mut steps: Vec<&BooleanArray> = Vec::with_capacity(self.steps.len());

        let b: Vec<Arc<dyn Array>> = self.steps.iter().map(|x| {
            if let ColumnarValue::Array(v) = x.evaluate(batch).unwrap() {
                return v;
            };
            panic!("unexpected");
        }).collect();

        for v in b.iter() {
            steps.push(v.as_any().downcast_ref::<BooleanArray>().unwrap())
        }

        let mut step_id: usize = 0;
        let mut row_id: usize = 0;
        let mut step = steps[step_id];
        while row_id < batch.num_rows() {
            if step.value(row_id) {
                step_id += 1;
                if step_id >= steps.len() {
                    return true;
                }
                step = steps[step_id];
            }
            row_id += 1;
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use arrow::datatypes::{Schema, Field, DataType};
    use arrow::array::Int8Array;
    use arrow::record_batch::RecordBatch;
    use datafusion::physical_plan::expressions::{Column, Literal, BinaryExpr};
    use datafusion::scalar::ScalarValue;
    use datafusion::logical_plan::Operator;
    use datafusion::{
        error::{Result},
    };
    use crate::expression_tree::sequence::Sequence;
    use crate::expression_tree::expr::Expr;

    #[test]
    fn test() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int8, false),
        ]));

        let a = Arc::new(Int8Array::from(vec![1, 2, 3]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                a.clone(),
            ],
        )?;

        let step1 = {
            let left = Column::new("a");
            let right = Literal::new(ScalarValue::Int8(Some(1)));
            BinaryExpr::new(Arc::new(left), Operator::Eq, Arc::new(right))
        };

        let step2 = {
            let left = Column::new("a");
            let right = Literal::new(ScalarValue::Int8(Some(2)));
            BinaryExpr::new(Arc::new(left), Operator::Eq, Arc::new(right))
        };

        let step3 = {
            let left = Column::new("a");
            let right = Literal::new(ScalarValue::Int8(Some(3)));
            BinaryExpr::new(Arc::new(left), Operator::Eq, Arc::new(right))
        };

        let op = Sequence::new(vec![Arc::new(step1), Arc::new(step2), Arc::new(step3)]);
        assert_eq!(true, op.evaluate(&batch, 0));
        Ok(())
    }
}