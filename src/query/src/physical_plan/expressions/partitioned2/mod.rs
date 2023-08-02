use std::collections::HashMap;
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
use arrow::buffer::ScalarBuffer;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::record_batch::RecordBatch;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion_expr::ColumnarValue;
use datafusion_expr::Operator;
use tracing::debug;

use crate::error::Result;
use crate::physical_plan::abs_row_id_refs;

// pub mod aggregate;
// pub mod aggregator;
// pub mod comparison;
// pub mod count;
// pub mod count2;
mod count;
pub mod count_grouped;
// mod funnel;
// mod count_hash;
// mod aggregate;
// pub mod aggregate;

use common::DECIMAL_SCALE;
use num::Integer;
use num_traits::Bounded;
use num_traits::Num;
use num_traits::NumCast;
use num_traits::PrimInt;
use num_traits::Zero;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

pub trait PartitionedAggregateExpr: Send + Sync {
    fn group_columns(&self) -> Vec<Column>;
    fn fields(&self) -> Vec<Field>;
    fn evaluate(&mut self, batch: &RecordBatch, partition_exist: &HashMap<i64, ()>) -> Result<()>;
    fn finalize(&mut self) -> Result<Vec<ArrayRef>>;
    fn make_new(&self) -> Result<Box<dyn PartitionedAggregateExpr>>;
}

#[derive(Debug, Clone)]
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
