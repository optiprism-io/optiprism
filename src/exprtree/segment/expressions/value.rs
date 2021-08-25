use arrow::array::{Array, ArrayRef, Int8Array};
use arrow::datatypes::{DataType, SchemaRef};
use datafusion::error::Result;
use datafusion::scalar::ScalarValue;
use std::marker::PhantomData;

use crate::exprtree::segment::expressions::boolean_op::BooleanOp;
use crate::exprtree::segment::expressions::expr::Expr;
pub use arrow::array::PrimitiveArray;
use arrow::datatypes::ArrowPrimitiveType;
use arrow::record_batch::RecordBatch;
use std::any::Any;
use std::ops::Deref;

pub struct Value {
    col_id: usize,
}

impl Value {
    pub fn new(col_id: usize) -> Self {
        Value { col_id }
    }
}

impl Expr<Option<i8>> for Value {
    fn evaluate(&self, batch: &RecordBatch, row_id: usize) -> Option<i8> {
        let arr = batch.columns()[self.col_id].as_ref();
        if arr.is_null(row_id) {
            None
        } else {
            Some(
                arr.as_any()
                    .downcast_ref::<Int8Array>()
                    .unwrap()
                    .value(row_id),
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int8Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    #[test]
    fn test_value() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("c", DataType::Int8, true)]));

        let c = Arc::new(Int8Array::from(vec![Some(1), Some(2), None]));
        let batch = RecordBatch::try_new(schema.clone(), vec![c.clone()])?;

        let op = Value::new(0);
        assert_eq!(Some(1), op.evaluate(&batch, 0));
        assert_eq!(Some(2), op.evaluate(&batch, 1));
        assert_eq!(None, op.evaluate(&batch, 2));
        Ok(())
    }
}
