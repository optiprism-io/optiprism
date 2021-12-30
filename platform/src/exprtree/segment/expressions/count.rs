use crate::exprtree::error::Result;
use crate::exprtree::segment::expressions::expr::Expr;
use crate::exprtree::segment::expressions::utils::into_array;
use arrow::array::{ArrayRef, BooleanArray};
use arrow::compute::kernels::arithmetic::{add, divide, divide_scalar, multiply, subtract};
use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::{ColumnarValue, PhysicalExpr};
use std::sync::Arc;

pub struct Count {
    predicate: Arc<dyn PhysicalExpr>,
}

impl Count {
    pub fn new(predicate: Arc<dyn PhysicalExpr>) -> Self {
        Count { predicate }
    }
}

impl Expr<i64> for Count {
    fn evaluate(&self, batch: &RecordBatch, _: usize) -> i64 {
        let ar = into_array(self.predicate.evaluate(batch).unwrap());
        let b = ar.as_any().downcast_ref::<BooleanArray>().unwrap();
        return b.iter().filter(|x| x.is_some() && x.unwrap()).count() as i64;
    }
}

#[cfg(test)]
mod tests {
    use crate::exprtree::segment::expressions::count::Count;
    use crate::exprtree::segment::expressions::expr::Expr;
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

        let a = Arc::new(Int8Array::from(vec![1, 0, 1]));
        let batch = RecordBatch::try_new(schema.clone(), vec![a.clone()])?;

        let left = Column::new_with_schema("a", &schema)?;
        let right = Literal::new(ScalarValue::Int8(Some(1)));
        let bo = BinaryExpr::new(Arc::new(left), Operator::Eq, Arc::new(right));
        let c = Count::new(Arc::new(bo));

        assert_eq!(2, c.evaluate(&batch, 0));
        Ok(())
    }
}
