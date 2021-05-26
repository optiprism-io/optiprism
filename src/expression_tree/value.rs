use std::marker::PhantomData;
use super::context::Context;
use arrow::array::{ArrayRef, Array, Int8Array};
use arrow::datatypes::{SchemaRef, DataType};
use datafusion::{
    error::{Result},
};
use datafusion::scalar::ScalarValue;

pub use arrow::array::PrimitiveArray;
use arrow::datatypes::ArrowPrimitiveType;
use std::ops::Deref;
use std::any::Any;
use crate::expression_tree::expr::{Expr};
use arrow::record_batch::RecordBatch;
use crate::expression_tree::boolean_op::BooleanOp;

pub struct Value {
    col_id: usize,
}

impl Value {
    pub fn new(col_id: usize) -> Self {
        Value {
            col_id,
        }
    }
}

impl Expr<Option<i8>> for Value {
    fn evaluate(&self, batch: &RecordBatch, row_id: usize) -> Option<i8> {
        let arr = batch.columns()[self.col_id].as_ref();
        if arr.is_null(row_id) {
            None
        } else {
            Some(arr.as_any().downcast_ref::<Int8Array>().unwrap().value(row_id))
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

    #[test]
    fn test_value() -> Result<()> {
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

        let op = Value::new(0);
        assert_eq!(Some(1), op.evaluate(&batch, 0));
        assert_eq!(Some(2), op.evaluate(&batch, 1));
        assert_eq!(None, op.evaluate(&batch, 2));
        Ok(())
    }
}