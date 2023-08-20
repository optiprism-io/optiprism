pub mod boolean_op;
pub mod comparison;
pub mod count;
pub mod time_range;

use arrow::array::Int64Array;
use arrow::buffer::ScalarBuffer;
use arrow::record_batch::RecordBatch;

use crate::error::Result;

pub trait SegmentExpr: Send + Sync {
    fn evaluate(
        &self,
        batch: &RecordBatch,
        partitions: &ScalarBuffer<i64>,
    ) -> Result<Option<Int64Array>>;
    fn finalize(&self) -> Result<Int64Array>;
}
