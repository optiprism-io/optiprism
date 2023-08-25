use arrow::array::ArrayRef;
use arrow::datatypes::Field;
use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::expressions::Column;

use crate::error::Result;

mod aggregate;
mod count;

pub trait AggregateExpr: Send + Sync {
    fn group_columns(&self) -> Vec<Column>;
    fn fields(&self) -> Vec<Field>;
    fn evaluate(&mut self, batch: &RecordBatch) -> Result<()>;
    fn finalize(&mut self) -> Result<Vec<ArrayRef>>;
    fn make_new(&self) -> Result<Box<dyn AggregateExpr>>;
}
