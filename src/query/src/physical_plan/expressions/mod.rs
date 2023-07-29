use arrow::array::Array;
use arrow::array::BooleanArray;
use num_traits::Bounded;
use num_traits::Zero;
use rust_decimal::Decimal;

pub mod aggregate;
pub mod partitioned;
mod partitioned2;
pub mod partitioned_aggregate;
pub mod partitioned_count;
pub mod partitioned_sum;
pub mod segmentation;
pub mod segmentation2;
pub mod sorted_distinct_count;
// psub mod funnel2;

// pub use funnel::test_utils::get_sample_events;

fn check_filter(filter: &BooleanArray, idx: usize) -> bool {
    if filter.is_null(idx) {
        return false;
    }
    filter.value(idx)
}
