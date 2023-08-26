use arrow::array::Array;
use arrow::array::BooleanArray;
use num_traits::Bounded;
use num_traits::Zero;

// pub mod _partitioned;
// pub mod _segmentation;
pub mod aggregate;
pub mod segmentation;
// psub mod funnel2;

// pub use funnel::test_utils::get_sample_events;

fn check_filter(filter: &BooleanArray, idx: usize) -> bool {
    if filter.is_null(idx) {
        return false;
    }
    filter.value(idx)
}
