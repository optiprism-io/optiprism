use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

pub trait Iterator {
    fn advance(&mut self) -> bool;
    fn current(&self, batch: &mut RecordBatch) -> bool;
    fn reset(&mut self);
    fn valid(&self) -> bool;
    fn prev(&mut self) -> bool;
    fn next(&mut self) -> Option<RecordBatch> {
        if !self.advance() {
            return None;
        }
        let mut batch = RecordBatch::new_empty(self.schema());
        if self.current(&mut batch) {
            Some(batch)
        } else {
            None
        }
    }

    fn schema(&self) -> SchemaRef;

    fn seek_to_first(&mut self) {
        self.reset();
        self.advance();
    }
}