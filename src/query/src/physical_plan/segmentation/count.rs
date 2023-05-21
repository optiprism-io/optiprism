use std::marker::PhantomData;
use std::sync::Arc;

use arrow::array::Array;
use arrow::array::ArrayRef;
use arrow::array::BooleanArray;
use arrow::compute::kernels;
use arrow::compute::kernels::arithmetic::add;
use arrow::compute::kernels::arithmetic::divide;
use arrow::compute::kernels::arithmetic::divide_scalar;
use arrow::compute::kernels::arithmetic::multiply;
use arrow::compute::kernels::arithmetic::subtract;
use arrow::datatypes::DataType;
use arrow::datatypes::Schema;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::ColumnarValue;
use datafusion::physical_plan::PhysicalExpr;
use datafusion_expr::Literal;

use crate::error::QueryError;
use crate::error::Result;
use crate::physical_plan::segmentation::boolean_op::BooleanOp;
use crate::physical_plan::segmentation::{Expr, RowResult, Spans};

#[derive(Debug)]
pub struct Count<Op> {
    predicate: Arc<dyn PhysicalExpr>,
    op: PhantomData<Op>,
    right: i64,
    acc: i64,
    spans: Spans,
}

impl<Op> Count<Op> {
    pub fn try_new(schema: &Schema, predicate: Arc<dyn PhysicalExpr>, right: i64) -> Result<Self> {
        match predicate.data_type(schema)? {
            DataType::Boolean => Ok(Count {
                predicate,
                op: PhantomData,
                right,
                acc: 0,
                spans: Spans::new(100),
            }),
            other => Err(QueryError::Plan(format!(
                "Count predicate must return boolean values, not {:?}",
                other,
            ))),
        }
    }
}

impl<Op> Expr for Count<Op>
    where Op: BooleanOp<i64>
{
    fn evaluate(
        &mut self,
        spans: &[usize],
        batch: &RecordBatch,
        is_last: bool,
    ) -> Result<Option<Vec<i64>>> {
        self.spans.reset(spans.to_vec(), batch.columns()[0].len());
        let arr = self.predicate.evaluate(batch)?.into_array(0);
        let arr = arr.as_any().downcast_ref::<BooleanArray>().unwrap();


        while self.spans.next_span() {
            while let Some(row) = self.spans.next_row() {
                match row {
                    RowResult::NextPartition(row_id) => {
                        if Op::perform(self.acc, self.right) || self.spans.cur_span() == 0 {
                            self.spans.push_result();
                        }

                        self.acc = 0;

                        if arr.value(row_id) {
                            self.acc += 1;
                        }

                        println!("acc {}", self.acc);

                        break;
                    }
                    RowResult::NextRow(row_id) => {
                        if arr.value(row_id as usize) {
                            self.acc += 1;
                        }
                        println!("acc {}", self.acc);
                    }
                }
            }
        }


        if is_last && self.spans.check_last_span() && Op::perform(self.acc, self.right) {
            self.spans.push_final_result()
        }
        Ok(self.spans.result())
    }
}

impl<Op: BooleanOp<i64>> std::fmt::Display for Count<Op> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", "Count")
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::ArrayRef;
    use arrow::array::BooleanArray;
    use arrow::array::Int8Array;
    use arrow::datatypes::DataType;
    use arrow::datatypes::Field;
    use arrow::datatypes::Schema;
    use arrow::record_batch::RecordBatch;
    use datafusion::error::Result;
    use datafusion::physical_expr::PhysicalExpr;
    use datafusion::physical_plan::expressions::BinaryExpr;
    use datafusion::physical_plan::expressions::Column;
    use datafusion::physical_plan::expressions::Literal;
    use datafusion_common::ScalarValue;

    use crate::physical_plan::segmentation::boolean_op::{BooleanEq, BooleanGt, BooleanNotEq};
    use crate::physical_plan::segmentation::count::Count;
    use crate::physical_plan::segmentation::Expr;

    #[test]
    fn one_batch() -> anyhow::Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Boolean, true)]));
        let col = Arc::new(Column::new_with_schema("a", &schema)?) as Arc<dyn PhysicalExpr>;
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(BooleanArray::from(vec![
                    true, true, true, true, true,
                ])) as ArrayRef
            ],
        )?;
        let mut op = Count::<BooleanEq>::try_new(&schema, col.clone(), 1)?;
        // 0, 1., 2, 0., 1
        assert_eq!(op.evaluate(&[1, 3], &batch, true)?, Some(vec![0, 3]));

        let mut op = Count::<BooleanNotEq>::try_new(&schema, col.clone(), 1)?;
        assert_eq!(op.evaluate(&[1, 3], &batch, false)?, Some(vec![2]));

        let mut op = Count::<BooleanNotEq>::try_new(&schema, col.clone(), 1)?;
        assert_eq!(op.evaluate(&[1, 3], &batch, true)?, Some(vec![2]));

        let mut op = Count::<BooleanGt>::try_new(&schema, col.clone(), 1)?;
        assert_eq!(op.evaluate(&[1, 3], &batch, false)?, Some(vec![2]));

        let mut op = Count::<BooleanGt>::try_new(&schema, col.clone(), 1)?;
        assert_eq!(op.evaluate(&[1, 3], &batch, true)?, Some(vec![2]));

        Ok(())
    }

    #[test]
    fn multiple_batches() -> anyhow::Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Boolean, true)]));
        let col = Arc::new(Column::new_with_schema("a", &schema)?) as Arc<dyn PhysicalExpr>;

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(BooleanArray::from(vec![
                    true, true,
                ])) as ArrayRef
            ],
        )?;
        let mut op = Count::<BooleanNotEq>::try_new(&schema, col.clone(), 1)?;
        assert_eq!(op.evaluate(&[1], &batch, false)?, None);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(BooleanArray::from(vec![
                    true, true,
                ])) as ArrayRef
            ],
        )?;
        assert_eq!(op.evaluate(&[], &batch, false)?, None);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(BooleanArray::from(vec![
                    true, true,
                ])) as ArrayRef
            ],
        )?;
        assert_eq!(op.evaluate(&[0], &batch, false)?, Some(vec![-1]));

        assert_eq!(op.evaluate(&[], &batch, false)?, None);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(BooleanArray::from(vec![
                    true, true,
                ])) as ArrayRef
            ],
        )?;
        assert_eq!(op.evaluate(&[1], &batch, false)?, Some(vec![0]));

        assert_eq!(op.evaluate(&[], &batch, false)?, None);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(BooleanArray::from(vec![
                    true,
                ])) as ArrayRef
            ],
        )?;
        assert_eq!(op.evaluate(&[0], &batch, false)?, Some(vec![-1]));

        Ok(())
    }
}
