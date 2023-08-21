mod aggregate;
// mod aggregate_decimal;
pub mod boolean_op;
pub mod comparison;
pub mod count;
pub mod time_range;

use arrow::array::BooleanArray;
use arrow::array::Int64Array;
use arrow::buffer::ScalarBuffer;
use arrow::record_batch::RecordBatch;
use num_traits::Bounded;
use num_traits::Num;
use num_traits::NumCast;
use num_traits::One;
use num_traits::Zero;
use rust_decimal::Decimal;

use crate::error::Result;

pub trait SegmentExpr: Send + Sync {
    fn evaluate(
        &self,
        batch: &RecordBatch,
        partitions: &ScalarBuffer<i64>,
    ) -> Result<Option<Int64Array>>;
    fn finalize(&self) -> Result<Int64Array>;
}
