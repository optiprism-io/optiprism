use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::marker::PhantomData;
use std::ops::Add;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef};
use arrow::array::BooleanArray;
use arrow::array::TimestampMillisecondArray;
use arrow::record_batch::RecordBatch;
use datafusion::physical_expr::PhysicalExpr;
use datafusion_expr::ColumnarValue;
use datafusion_expr::Operator;
use tracing::debug;

use crate::error::Result;
use crate::physical_plan::abs_row_id_refs;
use crate::Column;

pub mod aggregate;
mod aggregator;
pub mod comparison;
pub mod count;
pub mod time_range;

use num::Integer;
use num_traits::Bounded;
use num_traits::Num;
use num_traits::NumCast;
use num_traits::PrimInt;
use num_traits::Zero;

pub trait SegmentationExpr: Debug + Send + Sync {
    fn evaluate(&self, record_batch: &RecordBatch, hashes: &[u64]) -> Result<Option<ArrayRef>>;
    fn finalize(&self) -> Result<ArrayRef>;
}

#[derive(Debug, Clone)]
pub enum AggregateFunction<T>
where T: Copy + Num + Bounded + NumCast + PartialOrd + Clone
{
    Sum(T),
    Min(T),
    Max(T),
    Avg(T, T),
    Count(T),
}

impl<T> AggregateFunction<T>
where T: Copy + Num + Bounded + NumCast + PartialOrd + Clone
{
    pub fn new_sum() -> Self {
        AggregateFunction::Sum(T::zero())
    }

    pub fn new_min() -> Self {
        AggregateFunction::Min(T::max_value())
    }

    pub fn new_max() -> Self {
        AggregateFunction::Max(T::min_value())
    }

    pub fn new_avg() -> Self {
        AggregateFunction::Avg(T::zero(), T::zero())
    }

    pub fn new_count() -> Self {
        AggregateFunction::Count(T::zero())
    }

    pub fn accumulate(&mut self, v: T) -> T {
        match self {
            AggregateFunction::Sum(s) => {
                *s = *s + v;
                *s
            }
            AggregateFunction::Min(m) => {
                if v < *m {
                    *m = v;
                }
                *m
            }
            AggregateFunction::Max(m) => {
                if v > *m {
                    *m = v;
                }
                *m
            }
            AggregateFunction::Avg(s, c) => {
                *s = *s + v;
                *c = *c + T::one();
                *s / *c
            }
            AggregateFunction::Count(s) => {
                *s = *s + T::one();
                *s
            }
        }
    }

    pub fn result(&self) -> T {
        match self {
            AggregateFunction::Sum(s) => *s,
            AggregateFunction::Min(m) => *m,
            AggregateFunction::Max(m) => *m,
            AggregateFunction::Avg(s, c) => *s / *c,
            AggregateFunction::Count(s) => T::from(*s).unwrap(),
        }
    }
    pub fn reset(&mut self) {
        match self {
            AggregateFunction::Sum(s) => *s = T::zero(),
            AggregateFunction::Min(m) => *m = T::max_value(),
            AggregateFunction::Max(m) => *m = T::min_value(),
            AggregateFunction::Avg(s, c) => {
                *s = T::zero();
                *c = T::zero();
            }
            AggregateFunction::Count(s) => *s = T::zero(),
        }
    }
}

fn check_filter(filter: &BooleanArray, idx: usize) -> bool {
    if filter.is_null(idx) {
        return false;
    }
    filter.value(idx)
}