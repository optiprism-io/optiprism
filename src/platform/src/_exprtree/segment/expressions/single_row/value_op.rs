use crate::exprtree::segment::expressions::boolean_op::BooleanOp;
use crate::exprtree::segment::expressions::single_row::expr::Expr;
use arrow::array::Array;
use arrow::array::{ArrayRef, BooleanArray, Int64Array, Int8Array, StringArray};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::error::Result;
use datafusion::error::{DataFusionError, Result as DatafusionResult};
use std::any::{Any, TypeId};
use std::marker::PhantomData;
use std::sync::Arc;

pub struct ValueOp<T, Op> {
    left_col_id: usize,
    op: PhantomData<Op>,
    right: T,
}

trait TypeOf {}
impl<T, Op> ValueOp<T, Op> {
    pub fn try_new(schema: &Schema, left_col: &str, right: T) -> DatafusionResult<Self> {
        let a = std::any::type_name::<T>();

        let (left_col_id, left_field) = schema
            .column_with_name(left_col)
            .ok_or_else(|| DataFusionError::Plan(format!("Column {} not found", left_col)))?;

        // todo make validation
        /*match (left_field.data_type(), left_field.is_nullable()) {
            (DataType::Int8, true) => {
                /*if TypeId::of::<&right>() != TypeId::of::<Option<i8>>() {
                    return Err(DataFusionError::Plan("Left column must be comparable, not Option<i8>".to_string()));
                }*/
            }
            (DataType::Int8, false) => {}
            other => return Err(DataFusionError::Plan(format!(
                "Left column must be comparable, not {:?}",
                other,
            )))
        };*/
        Ok(ValueOp {
            left_col_id,
            op: PhantomData,
            right,
        })
    }
}

impl<Op> Expr for ValueOp<Option<&str>, Op>
where
    for<'a> Op: BooleanOp<Option<&'a str>>,
{
    fn evaluate(&self, batch: &RecordBatch, row_id: usize) -> DatafusionResult<bool> {
        let col = batch.columns()[self.left_col_id].as_ref();
        return if col.is_null(row_id) {
            Ok(Op::perform(None, self.right))
        } else {
            Ok(Op::perform(
                Some(
                    col.as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                        .value(row_id),
                ),
                self.right.clone(),
            ))
        };
    }
}

impl<Op> Expr for ValueOp<Option<i8>, Op>
where
    Op: BooleanOp<Option<i8>>,
{
    fn evaluate(&self, batch: &RecordBatch, row_id: usize) -> DatafusionResult<bool> {
        let col = batch.columns()[self.left_col_id].as_ref();
        return if col.is_null(row_id) {
            Ok(Op::perform(None, self.right))
        } else {
            Ok(Op::perform(
                Some(
                    col.as_any()
                        .downcast_ref::<Int8Array>()
                        .unwrap()
                        .value(row_id),
                ),
                self.right,
            ))
        };
    }
}

impl<Op> Expr for ValueOp<Option<i64>, Op>
where
    Op: BooleanOp<Option<i64>>,
{
    fn evaluate(&self, batch: &RecordBatch, row_id: usize) -> DatafusionResult<bool> {
        let col = batch.columns()[self.left_col_id].as_ref();
        return if col.is_null(row_id) {
            Ok(Op::perform(None, self.right))
        } else {
            let val = col
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(row_id);
            Ok(Op::perform(Some(val), self.right))
        };
    }
}

impl<Op> Expr for ValueOp<Option<bool>, Op>
where
    Op: BooleanOp<Option<bool>>,
{
    fn evaluate(&self, batch: &RecordBatch, row_id: usize) -> DatafusionResult<bool> {
        let col = batch.columns()[self.left_col_id].as_ref();
        return if col.is_null(row_id) {
            Ok(Op::perform(None, self.right))
        } else {
            Ok(Op::perform(
                Some(
                    col.as_any()
                        .downcast_ref::<BooleanArray>()
                        .unwrap()
                        .value(row_id),
                ),
                self.right,
            ))
        };
    }
}

impl<Op> Expr for ValueOp<i8, Op>
where
    Op: BooleanOp<i8>,
{
    fn evaluate(&self, batch: &RecordBatch, row_id: usize) -> DatafusionResult<bool> {
        let col = batch.columns()[self.left_col_id].as_ref();
        Ok(Op::perform(
            col.as_any()
                .downcast_ref::<Int8Array>()
                .unwrap()
                .value(row_id),
            self.right,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exprtree::segment::expressions::boolean_op::{Eq, Gt};
    use arrow::array::Int8Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    #[test]
    fn test_eq_nullable() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("c", DataType::Int8, true)]));

        let c = Arc::new(Int8Array::from(vec![Some(1), Some(2), None]));
        let batch = RecordBatch::try_new(schema.clone(), vec![c.clone()])?;

        let v1 = ValueOp::<Option<i8>, Eq>::try_new(&schema, "c", None)?;
        assert_eq!(false, v1.evaluate(&batch, 0)?);
        assert_eq!(false, v1.evaluate(&batch, 1)?);
        assert_eq!(true, v1.evaluate(&batch, 2)?);

        Ok(())
    }

    #[test]
    fn test_eq() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("c", DataType::Int8, false)]));

        let c = Arc::new(Int8Array::from(vec![1, 2, 0]));
        let batch = RecordBatch::try_new(schema.clone(), vec![c.clone()])?;

        let v1 = ValueOp::<i8, Eq>::try_new(&schema, "c", 0)?;
        assert_eq!(false, v1.evaluate(&batch, 0)?);
        assert_eq!(false, v1.evaluate(&batch, 1)?);
        assert_eq!(true, v1.evaluate(&batch, 2)?);

        Ok(())
    }

    #[test]
    fn test_string() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("c", DataType::Utf8, true)]));

        let c = Arc::new(StringArray::from(vec![Some("a"), Some("b"), None]));
        let batch = RecordBatch::try_new(schema.clone(), vec![c.clone()])?;

        let v1 = ValueOp::<Option<&str>, Eq>::try_new(&schema, "c", Some("a"))?;
        assert_eq!(true, v1.evaluate(&batch, 0)?);
        assert_eq!(false, v1.evaluate(&batch, 1)?);
        assert_eq!(false, v1.evaluate(&batch, 2)?);

        Ok(())
    }

    #[test]
    fn test_gt() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("c", DataType::Int8, true)]));

        let c = Arc::new(Int8Array::from(vec![Some(1), Some(2), None]));
        let batch = RecordBatch::try_new(schema.clone(), vec![c.clone()])?;

        let v1 = ValueOp::<Option<i8>, Gt>::try_new(&schema, "c", Some(1i8))?;
        assert_eq!(false, v1.evaluate(&batch, 0)?);
        assert_eq!(true, v1.evaluate(&batch, 1)?);
        assert_eq!(false, v1.evaluate(&batch, 2)?);

        Ok(())
    }
}
