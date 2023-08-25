use ahash::HashMap;
use ahash::RandomState;
use arrow::array::ArrayRef;
use arrow::datatypes::Field;
use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::expressions::Column;
use num_traits::Zero;
use rust_decimal::Decimal;

use crate::error::Result;

pub mod aggregate;
pub mod count;
pub mod partitioned;

pub trait AggregateExpr: Send + Sync {
    fn group_columns(&self) -> Vec<Column>;
    fn fields(&self) -> Vec<Field>;
    fn evaluate(&mut self, batch: &RecordBatch) -> Result<()>;
    fn finalize(&mut self) -> Result<Vec<ArrayRef>>;
    fn make_new(&self) -> Result<Box<dyn AggregateExpr>>;
}

pub trait PartitionedAggregateExpr: Send + Sync {
    fn group_columns(&self) -> Vec<Column>;
    fn fields(&self) -> Vec<Field>;
    fn evaluate(
        &mut self,
        batch: &RecordBatch,
        partition_exist: Option<&HashMap<i64, ()>>,
    ) -> Result<()>;
    fn finalize(&mut self) -> Result<Vec<ArrayRef>>;
    fn make_new(&self) -> Result<Box<dyn PartitionedAggregateExpr>>;
}

#[derive(Debug)]
pub enum AggregateFunction {
    Sum(i128),
    Min(i128),
    Max(i128),
    Avg(i128, i128),
    Count(i128),
}

impl AggregateFunction {
    pub fn new_sum() -> Self {
        AggregateFunction::Sum(i128::zero())
    }

    pub fn new_min() -> Self {
        AggregateFunction::Min(i128::max_value())
    }

    pub fn new_max() -> Self {
        AggregateFunction::Max(i128::min_value())
    }

    pub fn new_avg() -> Self {
        AggregateFunction::Avg(i128::zero(), i128::zero())
    }

    pub fn new_count() -> Self {
        AggregateFunction::Count(i128::zero())
    }

    pub fn make_new(&self) -> Self {
        match self {
            AggregateFunction::Sum(_) => AggregateFunction::new_sum(),
            AggregateFunction::Min(_) => AggregateFunction::new_min(),
            AggregateFunction::Max(_) => AggregateFunction::new_max(),
            AggregateFunction::Avg(_, _) => AggregateFunction::new_avg(),
            AggregateFunction::Count(_) => AggregateFunction::new_count(),
        }
    }

    pub fn accumulate(&mut self, v: i128) {
        match self {
            AggregateFunction::Sum(s) => {
                *s = *s + v;
            }
            AggregateFunction::Min(m) => {
                if v < *m {
                    *m = v;
                }
            }
            AggregateFunction::Max(m) => {
                if v > *m {
                    *m = v;
                }
            }
            AggregateFunction::Avg(s, c) => {
                *s = *s + v;
                *c = *c + 1;
            }
            AggregateFunction::Count(s) => {
                *s = *s + 1;
            }
        }
    }

    pub fn result(&self) -> i128 {
        match self {
            AggregateFunction::Sum(s) => *s,
            AggregateFunction::Min(m) => *m,
            AggregateFunction::Max(m) => *m,
            AggregateFunction::Avg(s, c) => {
                let v =
                    Decimal::from_i128_with_scale(*s, 10) / Decimal::from_i128_with_scale(*c, 10);
                v.mantissa()
            }
            AggregateFunction::Count(s) => *s,
        }
    }
    pub fn reset(&mut self) {
        match self {
            AggregateFunction::Sum(s) => *s = i128::zero(),
            AggregateFunction::Min(m) => *m = i128::max_value(),
            AggregateFunction::Max(m) => *m = i128::min_value(),
            AggregateFunction::Avg(s, c) => {
                *s = i128::zero();
                *c = i128::zero();
            }
            AggregateFunction::Count(s) => *s = i128::zero(),
        }
    }
}
