use std::marker::PhantomData;
use crate::expression_tree::enum_dispatch::expr::Expr;
use arrow::record_batch::RecordBatch;
use arrow::array::{Int8Array, StringArray, ArrayRef, BooleanArray, Int64Array};
use crate::expression_tree::enum_dispatch::boolean_op::BooleanOp;
use datafusion::{
    error::{Result},
};
use std::sync::Arc;

use arrow::array::Array;
use arrow::datatypes::DataType;


pub struct ValueOp<T, Op> {
    left_col_id: usize,
    op: PhantomData<Op>,
    right: T,
}

impl<T, Op> ValueOp<T, Op> {
    pub fn new(left_col_id: usize, right: T) -> Self {
        ValueOp {
            left_col_id,
            op: PhantomData,
            right,
        }
    }
}

impl<Op> Expr<bool> for ValueOp<Option<&str>, Op> where for<'a> Op: BooleanOp<Option<&'a str>> {
    fn evaluate(&self, batch: &RecordBatch, row_id: usize) -> bool {
        let col = batch.columns()[self.left_col_id].as_ref();
        return if col.is_null(row_id) {
            Op::perform(None, self.right)
        } else {
            Op::perform(Some(col.as_any().downcast_ref::<StringArray>().unwrap().value(row_id)), self.right.clone())
        };
    }
}

impl<Op> Expr<bool> for ValueOp<Option<i8>, Op> where Op: BooleanOp<Option<i8>> {
    fn evaluate(&self, batch: &RecordBatch, row_id: usize) -> bool {
        let col = batch.columns()[self.left_col_id].as_ref();
        return if col.is_null(row_id) {
            Op::perform(None, self.right)
        } else {
            Op::perform(Some(col.as_any().downcast_ref::<Int8Array>().unwrap().value(row_id)), self.right)
        };
    }
}

impl<Op> Expr<bool> for ValueOp<Option<i64>, Op> where Op: BooleanOp<Option<i64>> {
    fn evaluate(&self, batch: &RecordBatch, row_id: usize) -> bool {
        let col = batch.columns()[self.left_col_id].as_ref();
        return if col.is_null(row_id) {
            Op::perform(None, self.right)
        } else {
            let val = col.as_any().downcast_ref::<Int64Array>().unwrap().value(row_id);
            Op::perform(Some(val), self.right)
        };
    }
}

impl<Op> Expr<bool> for ValueOp<Option<bool>, Op> where Op: BooleanOp<Option<bool>> {
    fn evaluate(&self, batch: &RecordBatch, row_id: usize) -> bool {
        let col = batch.columns()[self.left_col_id].as_ref();
        return if col.is_null(row_id) {
            Op::perform(None, self.right)
        } else {
            Op::perform(Some(col.as_any().downcast_ref::<BooleanArray>().unwrap().value(row_id)), self.right)
        };
    }
}

impl<Op> Expr<bool> for ValueOp<i8, Op> where Op: BooleanOp<i8> {
    fn evaluate(&self, batch: &RecordBatch, row_id: usize) -> bool {
        let col = batch.columns()[self.left_col_id].as_ref();
        Op::perform(col.as_any().downcast_ref::<Int8Array>().unwrap().value(row_id), self.right)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int8Array;
    use std::sync::Arc;
    use arrow::datatypes::{Schema, Field, DataType};
    use arrow::record_batch::RecordBatch;
    use crate::expression_tree::enum_dispatch::boolean_op::{Eq, Gt};

    #[test]
    fn test_eq_nullable() -> Result<()> {
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

        let v1 = ValueOp::<Option<i8>, Eq>::new(0, None);
        assert_eq!(false, v1.evaluate(&batch, 0));
        assert_eq!(false, v1.evaluate(&batch, 1));
        assert_eq!(true, v1.evaluate(&batch, 2));

        Ok(())
    }

    #[test]
    fn test_eq() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("c", DataType::Int8, false),
        ]));

        let c = Arc::new(Int8Array::from(vec![1, 2, 0]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                c.clone(),
            ],
        )?;

        let v1 = ValueOp::<i8, Eq>::new(0, 0);
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

        let v1 = ValueOp::<Option<&str>, Eq>::new(0, Some("a"));
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

        let v1 = ValueOp::<Option<i8>, Gt>::new(0, Some(1i8));
        assert_eq!(false, v1.evaluate(&batch, 0));
        assert_eq!(true, v1.evaluate(&batch, 1));
        assert_eq!(false, v1.evaluate(&batch, 2));

        Ok(())
    }
}