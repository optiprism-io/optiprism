use crate::exprtree::error::Result;
use crate::exprtree::segment::expressions::expr::Expr;
use crate::exprtree::segment::expressions::utils::into_array;
use crate::exprtree::segment::expressions::value::Value;
use arrow::array::{Array, ArrayRef, BooleanArray, Int8Array};
use arrow::compute::kernels::arithmetic::{add, divide, divide_scalar, multiply, subtract};
use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::{ColumnarValue, PhysicalExpr};
use std::ops::{Add, AddAssign};
use std::sync::Arc;

pub struct Sum {
    col_id: usize,
    predicate: Arc<dyn PhysicalExpr>,
}

impl Sum {
    pub fn new(col_id: usize, predicate: Arc<dyn PhysicalExpr>) -> Self {
        Sum { col_id, predicate }
    }
}

impl Expr<i8> for Sum {
    fn evaluate(&self, batch: &RecordBatch, _: usize) -> i8 {
        let ar = into_array(self.predicate.evaluate(batch).unwrap());
        let b = ar.as_any().downcast_ref::<BooleanArray>().unwrap();
        let v = batch.columns()[self.col_id]
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap();
        return b
            .iter()
            .enumerate()
            .filter(|(i, x)| x.is_some() && x.unwrap() && !v.data_ref().is_null(*i))
            .map(|(i, _)| v.value(i))
            .fold(0i8, |acc, x| acc + x);
    }
}

#[cfg(test)]
mod tests {
    use crate::exprtree::segment::expressions::count::Count;
    use crate::exprtree::segment::expressions::expr::Expr;
    use crate::exprtree::segment::expressions::sum::Sum;
    use arrow::array::{ArrayRef, BooleanArray, Int8Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::error::Result;
    use datafusion::logical_plan::Operator;
    use datafusion::physical_plan::expressions::{BinaryExpr, Column, Literal};
    use datafusion::scalar::ScalarValue;
    use std::sync::Arc;

    #[test]
    fn test() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int8, false)]));

        let a = Arc::new(Int8Array::from(vec![1, 3, 1]));
        let batch = RecordBatch::try_new(schema.clone(), vec![a.clone()])?;

        let left = Column::new_with_schema("a", &schema)?;
        let right = Literal::new(ScalarValue::Int8(Some(1)));
        let bo = BinaryExpr::new(Arc::new(left), Operator::Eq, Arc::new(right));
        let c = Sum::new(0, Arc::new(bo));

        assert_eq!(2, c.evaluate(&batch, 0));
        Ok(())
    }
}
