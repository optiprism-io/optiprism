use arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;

pub trait RecordBatchWriter {
    fn apply_schema(&mut self, schema: &Schema) -> Vec<usize>,
    fn write<T: AsRef<[u8]>>(&mut self, data: T) -> Result<()>;
    fn build(&self) -> RecordBatch;
}