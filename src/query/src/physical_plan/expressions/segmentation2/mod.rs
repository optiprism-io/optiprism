mod boolean_op;
mod comparison;
mod count;
mod time_range;

use arrow::array::Int64Array;
use arrow::buffer::ScalarBuffer;
use arrow::record_batch::RecordBatch;

use crate::error::Result;

pub trait SegmentExpr<'a> {
    fn evaluate(
        &self,
        batch: &RecordBatch,
        partitions: &ScalarBuffer<i64>,
    ) -> Result<Option<Int64Array>>;
    fn finalize(&self) -> Result<Int64Array>;
}
