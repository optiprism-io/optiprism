use std::fmt::Debug;
use datafusion::arrow::array::ArrayRef;

pub mod aggregate_partitioned;
pub mod sum;
pub mod average;
pub mod count;