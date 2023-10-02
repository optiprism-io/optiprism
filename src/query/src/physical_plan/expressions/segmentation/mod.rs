pub mod aggregate;
// mod aggregate_decimal;
pub mod boolean_op;
pub mod comparison;
pub mod count;
pub mod time_range;

use std::fmt::Debug;


use arrow::array::Int64Array;
use arrow::buffer::ScalarBuffer;
use arrow::record_batch::RecordBatch;







use crate::error::Result;

pub trait SegmentExpr: Send + Sync + Debug {
    fn evaluate(
        &self,
        batch: &RecordBatch,
        partitions: &ScalarBuffer<i64>,
    ) -> Result<Option<Int64Array>>;
    fn finalize(&self) -> Result<Int64Array>;
}
