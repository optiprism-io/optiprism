use std::marker::PhantomData;
use crate::expression_tree::expr::Expr;
use arrow::record_batch::RecordBatch;
use arrow::array::{Int8Array, StringArray, ArrayRef};
use crate::expression_tree::boolean_op::BooleanOp;
use datafusion::{
    error::{Result},
};
use std::sync::Arc;

use arrow::array::Array;

enum Arr<'a> {
    String(&'a StringArray)
}

pub struct ValueOp<'a, T, Op> {
    left: Arr<'a>,
    op: PhantomData<Op>,
    right: Option<T>,
}

impl<'a, T, Op> ValueOp<'a, T, Op> {
    pub fn new(col: &'a ArrayRef, right: Option<T>) -> Self {
        ValueOp {
            left: Arr::String(col.as_any().downcast_ref::<StringArray>().unwrap()),
            op: PhantomData,
            right,
        }
    }
}

/*impl<'a, Op> Expr<bool> for ValueOp<'a, i8, Op> where Op: BooleanOp<Option<i8>> {
    fn evaluate(&self, _: &RecordBatch, row_id: usize) -> bool {
        let arr = batch.columns()[self.col_id].as_ref();
        if arr.is_null(row_id) {
            Op::perform(None, self.right)
        } else {
            Op::perform(Some(arr.as_any().downcast_ref::<Int8Array>().unwrap().value(row_id)), self.right)
        }
    }
}

*/impl<'a, Op> Expr<bool> for ValueOp<'a, &str, Op> where for<'b> Op: BooleanOp<Option<&'b str>> {
    fn evaluate(&self, batch: &RecordBatch, row_id: usize) -> bool {
        let Arr::String(arr) = self.left;

        return if arr.is_null(row_id) {
            Op::perform(None, self.right)
        } else {
            Op::perform(Some(arr.value(row_id)), self.right.clone())
        };
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
    /*
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
    */
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

        let v = c as ArrayRef;
        let v1 = ValueOp::<&str, Eq>::new(&v, Some("a"));
        assert_eq!(true, v1.evaluate(&batch, 0));
        assert_eq!(false, v1.evaluate(&batch, 1));
        assert_eq!(false, v1.evaluate(&batch, 2));


        Ok(())
    }
    /*
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
        }*/
}