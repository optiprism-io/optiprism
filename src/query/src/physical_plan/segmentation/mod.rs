use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::marker::PhantomData;
use std::ops::Add;
use std::sync::Arc;

use arrow::array::ArrayRef;
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

// pub mod aggregate_column;
pub mod aggregate_column;
mod boolean_op;
pub mod count_predicate;
pub mod count_predicate_relative;
pub mod logical_op;
pub mod time_range;

use num::Integer;
use num_traits::Bounded;
use num_traits::Num;
use num_traits::NumCast;
use num_traits::PrimInt;
use num_traits::Zero;

pub trait SegmentationExpr {
    fn evaluate(
        &mut self,
        record_batches: &[RecordBatch],
        spans: Vec<usize>,
        skip: usize,
    ) -> Result<Vec<bool>>;
}

pub trait Primitive: Copy + Num + Bounded + NumCast + PartialOrd + Clone {}

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

#[macro_export]
macro_rules! span {
    ($batch:ident) => {
        // Span is a span of rows that are in the same partition
#[derive(Debug, Clone)]
pub struct Span<'a> {
    id: usize,                // # of span
    offset: usize, // offset of the span. Used to skip rows from record batch. See PartitionedState
    len: usize,    // length of the span
    batches: &'a [$batch<'a>], // one or more batches with span
    row_id: usize, // current row id
}

impl<'a> Span<'a> {
    pub fn new(id: usize, offset: usize, len: usize, batches: &'a [$batch]) -> Self {
        Self {
            id,
            offset,
            len,
            batches,
            row_id: 0,
        }
    }

    #[inline]
    pub fn abs_row_id(&self) -> (usize, usize) {
        abs_row_id_refs(
            self.row_id + self.offset,
            self.batches.iter().map(|b| b.batch).collect::<Vec<_>>(),
        )
    }

    // get ts value of current row
    #[inline]
    pub fn ts_value(&self) -> i64 {
        // calculate batch id and row id
        let (batch_id, idx) = self.abs_row_id();
        self.batches[batch_id].ts.value(idx)
    }

    #[inline]
    pub fn check_predicate(&self) -> bool {
        let (batch_id, idx) = self.abs_row_id();
        self.batches[batch_id].predicate.value(idx)
    }

    // go to next row
    #[inline]
    pub fn next_row(&mut self) -> bool {
        if self.row_id == self.len - 1 {
            return false;
        }
        self.row_id += 1;

        true
    }

    #[inline]
    pub fn is_next_row(&self) -> bool {
        if self.row_id >= self.len - 1 {
            return false;
        }

        true
    }
}
    }
}
