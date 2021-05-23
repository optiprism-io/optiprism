use std::marker::PhantomData;
use crate::expression_tree::expr::Expr;
use arrow::record_batch::RecordBatch;
use arrow::array::{Int8Array, StringArray};
use crate::expression_tree::boolean_op::BooleanOp;
use datafusion::{
    error::{Result},
};

pub struct ValueOp<T, Op> {
    col_id: usize,
    op: PhantomData<Op>,
    right: Option<T>,
}

impl<T, Op> ValueOp<T, Op> {
    pub fn new(col_id: usize, right: Option<T>) -> Self {
        ValueOp {
            col_id,
            op: PhantomData,
            right,
        }
    }
}

impl<Op> Expr<bool> for ValueOp<i8, Op> where Op: BooleanOp<Option<i8>> {
    fn evaluate(&self, batch: &RecordBatch, row_id: usize) -> bool {
        let arr = batch.columns()[self.col_id].as_ref();
        if arr.is_null(row_id) {
            Op::perform(None, self.right)
        } else {
            Op::perform(Some(arr.as_any().downcast_ref::<Int8Array>().unwrap().value(row_id)), self.right)
        }
    }
}

impl<Op> Expr<bool> for ValueOp<String, Op> where Op: BooleanOp<Option<String>> {
    fn evaluate(&self, batch: &RecordBatch, row_id: usize) -> bool {
        let arr = batch.columns()[self.col_id].as_ref();
        if arr.is_null(row_id) {
            Op::perform(None, self.right.clone())
        } else {
            Op::perform(Some(arr.as_any().downcast_ref::<StringArray>().unwrap().value(row_id).to_string()), self.right.clone())
        }
    }
}

impl<Op> Expr<bool> for ValueOp<&str, Op> where Op: BooleanOp<Option<&str>> {
    fn evaluate(&self, batch: &RecordBatch, row_id: usize) -> bool {
        let arr = batch.columns()[self.col_id].as_ref();
        if arr.is_null(row_id) {
            Op::perform(None, self.right)
        } else {
            Op::perform(Some(arr.as_any().downcast_ref::<StringArray>().unwrap().value(row_id)), self.right.clone())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int8Array;
    use std::sync::Arc;
    use arrow::datatypes::{Schema, Field, DataType};
    use arrow::record_batch::RecordBatch;
    use crate::expression_tree::boolean_op::{Eq, Gt};

    #[test]
    fn test_eq() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("c", DataType::Int8, true),
        ]));

        let c = Arc::new(Int8Array::from(vec![Some(1), Some(2), None]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                c.clone(),
            ],
        )?;

        let v1 = ValueOp::<i8, Eq>::new(0, None);
        assert_eq!(false, v1.evaluate(&batch, 0));
        assert_eq!(false, v1.evaluate(&batch, 1));
        assert_eq!(true, v1.evaluate(&batch, 2));

        Ok(())
    }

    #[test]
    fn test_string() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("c", DataType::Utf8, true),
        ]));

        let c = Arc::new(StringArray::from(vec![Some("a"), Some("b"), None]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                c.clone(),
            ],
        )?;

        let v1 = ValueOp::<String, Eq>::new(0, Some("a".to_string()));
        assert_eq!(true, v1.evaluate(&batch, 0));
        assert_eq!(false, v1.evaluate(&batch, 1));
        assert_eq!(false, v1.evaluate(&batch, 2));


        Ok(())
    }

    #[test]
    fn test_gt() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("c", DataType::Int8, true),
        ]));

        let c = Arc::new(Int8Array::from(vec![Some(1), Some(2), None]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                c.clone(),
            ],
        )?;

        let v1 = ValueOp::<_, Gt>::new(0, Some(1));
        assert_eq!(false, v1.evaluate(&batch, 0));
        assert_eq!(true, v1.evaluate(&batch, 1));
        assert_eq!(false, v1.evaluate(&batch, 2));

        Ok(())
    }
}