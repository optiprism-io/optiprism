use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::marker::PhantomData;
use std::ops::Add;
use std::sync::Arc;

use arrow::array::Array;
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

// pub mod aggregate;
// pub mod aggregator;
// pub mod comparison;
// pub mod count;
pub mod boolean_op;
pub mod count;
pub mod time_range;
// mod aggregate;
// pub mod aggregate;
pub mod aggregate;
mod aggregate_aggregate;
pub mod comparison;
pub mod cond_aggregate;
pub mod cond_count;
mod count_aggregate;
pub mod funnel;

use common::DECIMAL_SCALE;
use num::Integer;
use num_traits::Bounded;
use num_traits::Num;
use num_traits::NumCast;
use num_traits::PrimInt;
use num_traits::Zero;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

pub trait SegmentationExpr: Debug + Send + Sync {
    fn evaluate(&self, record_batch: &RecordBatch, hashes: &[u64]) -> Result<Option<BooleanArray>>;
    fn finalize(&self) -> Result<BooleanArray>;
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

#[derive(Debug, Clone)]
pub enum AggregateFunction2 {
    Sum(i128),
    Min(i128),
    Max(i128),
    Avg(i128, i128),
    Count(i128),
}

impl AggregateFunction2 {
    pub fn new_sum() -> Self {
        AggregateFunction2::Sum(i128::zero())
    }

    pub fn new_min() -> Self {
        AggregateFunction2::Min(i128::max_value())
    }

    pub fn new_max() -> Self {
        AggregateFunction2::Max(i128::min_value())
    }

    pub fn new_avg() -> Self {
        AggregateFunction2::Avg(i128::zero(), i128::zero())
    }

    pub fn new_count() -> Self {
        AggregateFunction2::Count(i128::zero())
    }

    pub fn accumulate(&mut self, v: i128) {
        match self {
            AggregateFunction2::Sum(s) => {
                *s = *s + v;
            }
            AggregateFunction2::Min(m) => {
                if v < *m {
                    *m = v;
                }
            }
            AggregateFunction2::Max(m) => {
                if v > *m {
                    *m = v;
                }
            }
            AggregateFunction2::Avg(s, c) => {
                *s = *s + v;
                *c = *c + 1;
            }
            AggregateFunction2::Count(s) => {
                *s = *s + 1;
            }
        }
    }

    pub fn result(&self) -> i128 {
        match self {
            AggregateFunction2::Sum(s) => *s,
            AggregateFunction2::Min(m) => *m,
            AggregateFunction2::Max(m) => *m,
            AggregateFunction2::Avg(s, c) => {
                let v =
                    Decimal::from_i128_with_scale(*s, 10) / Decimal::from_i128_with_scale(*c, 10);
                v.mantissa()
            }
            AggregateFunction2::Count(s) => *s,
        }
    }
    pub fn reset(&mut self) {
        match self {
            AggregateFunction2::Sum(s) => *s = i128::zero(),
            AggregateFunction2::Min(m) => *m = i128::max_value(),
            AggregateFunction2::Max(m) => *m = i128::min_value(),
            AggregateFunction2::Avg(s, c) => {
                *s = i128::zero();
                *c = i128::zero();
            }
            AggregateFunction2::Count(s) => *s = i128::zero(),
        }
    }
}

pub enum AggregateFunction3 {
    Count,
    Sum,
    Min,
    Max,
    Avg,
}

pub struct Accumulator<T, O> {
    agg_fn: AggregateFunction3,
    v1: T,
    v2: T,
    o: PhantomData<O>,
}

impl<T, O> Accumulator<T, O>
where
    T: Copy + Num + Bounded + NumCast + PartialOrd + Clone,
    O: Copy + Num + Bounded + NumCast + PartialOrd + Clone,
{
    pub fn new(agg_fn: AggregateFunction3) -> Self {
        let v1 = {
            match agg_fn {
                AggregateFunction3::Min => T::max_value(),
                AggregateFunction3::Max => T::min_value(),
                _ => T::zero(),
            }
        };
        Self {
            agg_fn,
            v1,
            v2: T::zero(),
            o: Default::default(),
        }
    }
    pub fn accumulate(&mut self, v: T) {
        match self.agg_fn {
            AggregateFunction3::Count => self.v1 = self.v1 + T::one(),
            AggregateFunction3::Sum => self.v1 = self.v1 + v,
            AggregateFunction3::Min if v < self.v1 => self.v1 = v,
            AggregateFunction3::Max if v > self.v1 => self.v1 = v,
            AggregateFunction3::Avg => {
                self.v1 = self.v1 + v;
                self.v2 = self.v2 + T::one();
            }
            _ => {}
        }
    }

    pub fn result(&mut self) -> O {
        match self.agg_fn {
            AggregateFunction3::Count => O::from(self.v1).unwrap(),
            AggregateFunction3::Sum => O::from(self.v1).unwrap(),
            AggregateFunction3::Min => O::from(self.v1).unwrap(),
            AggregateFunction3::Max => O::from(self.v1).unwrap(),
            AggregateFunction3::Avg => {
                let v = Decimal::from_i128_with_scale(
                    <i128 as NumCast>::from(self.v1).unwrap(),
                    DECIMAL_SCALE as u32,
                ) / Decimal::from_i128_with_scale(
                    <i128 as NumCast>::from(self.v2).unwrap(),
                    DECIMAL_SCALE as u32,
                );
                O::from(v.mantissa()).unwrap()
            }
        }
    }

    pub fn reset(&mut self) {
        self.v1 = T::zero();
        self.v2 = T::zero();
    }
}

fn check_filter(filter: &BooleanArray, idx: usize) -> bool {
    if filter.is_null(idx) {
        return false;
    }
    filter.value(idx)
}

#[cfg(test)]
mod tests {
    use common::DECIMAL_SCALE;
    use rust_decimal::Decimal;

    use crate::physical_plan::expressions::partitioned::Accumulator;
    use crate::physical_plan::expressions::partitioned::AggregateFunction3;

    #[test]
    fn it_works() {
        let mut acc = Accumulator::<i64, i128>::new(AggregateFunction3::Count);
        acc.accumulate(1);
        acc.accumulate(1);
        assert_eq!(acc.result(), 2);

        let mut acc = Accumulator::<i64, i128>::new(AggregateFunction3::Min);
        acc.accumulate(1);
        acc.accumulate(2);
        assert_eq!(acc.result(), 1);

        let mut acc = Accumulator::<i64, i128>::new(AggregateFunction3::Max);
        acc.accumulate(1);
        acc.accumulate(2);
        assert_eq!(acc.result(), 2);

        let mut acc = Accumulator::<i64, i128>::new(AggregateFunction3::Avg);
        acc.accumulate(2);
        acc.accumulate(2);
        acc.accumulate(1);

        let exp = Decimal::from_i128_with_scale(5, DECIMAL_SCALE as u32)
            / Decimal::from_i128_with_scale(3, DECIMAL_SCALE as u32);
        assert_eq!(exp.mantissa(), acc.result());

        let mut acc = Accumulator::<i64, i128>::new(AggregateFunction3::Max);
        acc.accumulate(1);
        acc.accumulate(2);
        acc.reset();
        assert_eq!(acc.result(), 0);
        acc.accumulate(1);
        acc.accumulate(2);
        assert_eq!(acc.result(), 2);
    }
}
