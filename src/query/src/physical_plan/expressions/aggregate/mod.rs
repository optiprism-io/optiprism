use std::fmt::Debug;
use std::marker::PhantomData;

use ahash::HashMap;
use ahash::RandomState;
use arrow::array::ArrayRef;
use arrow::datatypes::Field;
use arrow::record_batch::RecordBatch;
use arrow_row::OwnedRow;
use arrow_row::RowConverter;
use arrow_row::SortField;
use common::DECIMAL_SCALE;
use datafusion::physical_expr::PhysicalExprRef;
use datafusion::physical_plan::expressions::Column;
use num_traits::AsPrimitive;
use num_traits::Bounded;
use num_traits::Num;
use num_traits::NumAssign;
use num_traits::NumCast;
use num_traits::Zero;
use rust_decimal::Decimal;
use store::test_util::PrimaryIndexType;

use crate::error::Result;

pub mod aggregate;
pub mod count;
pub mod partitioned;

#[derive(Debug)]
struct Groups<T> {
    exprs: Vec<PhysicalExprRef>,
    names: Vec<String>,
    sort_fields: Vec<SortField>,
    row_converter: RowConverter,
    groups: HashMap<OwnedRow, T>,
}

impl<T> Groups<T> {
    pub fn try_make_new(&self) -> Result<Self> {
        Ok(Groups {
            exprs: self.exprs.clone(),
            names: self.names.clone(),
            sort_fields: self.sort_fields.clone(),
            row_converter: RowConverter::new(self.sort_fields.clone())?,
            groups: Default::default(),
        })
    }

    pub fn maybe_from(
        groups: Option<Vec<(PhysicalExprRef, String, SortField)>>,
    ) -> Result<Option<Self>> {
        if let Some(groups) = groups {
            Ok(Some(Self {
                exprs: groups.iter().map(|(v, _, _)| v.clone()).collect::<Vec<_>>(),
                names: groups.iter().map(|(_, v, _)| v.to_owned()).collect(),
                sort_fields: groups.iter().map(|(_, _, v)| v.clone()).collect(),
                row_converter: RowConverter::new(
                    groups.iter().map(|(_, _, s)| s.clone()).collect(),
                )?,
                groups: Default::default(),
            }))
        } else {
            Ok(None)
        }
    }
}

pub trait PartitionedAggregateExpr: Send + Sync + Debug {
    fn group_columns(&self) -> Vec<(PhysicalExprRef, String)>;
    fn fields(&self) -> Vec<Field>;
    fn evaluate(
        &mut self,
        batch: &RecordBatch,
        partition_exist: Option<&HashMap<i64, ()>>,
    ) -> Result<()>;
    fn finalize(&mut self) -> Result<Vec<ArrayRef>>;
    fn make_new(&self) -> Result<Box<dyn PartitionedAggregateExpr>>;
}
// #[derive(Debug)]
// pub enum AggregateFunction {
// Sum(i128),
// Min(i128),
// Max(i128),
// Avg(i128, i128),
// Count(i128),
// }
//
// impl AggregateFunction {
// pub fn new_sum() -> Self {
// AggregateFunction::Sum(i128::zero())
// }
//
// pub fn new_min() -> Self {
// AggregateFunction::Min(i128::max_value())
// }
//
// pub fn new_max() -> Self {
// AggregateFunction::Max(i128::min_value())
// }
//
// pub fn new_avg() -> Self {
// AggregateFunction::Avg(i128::zero(), i128::zero())
// }
//
// pub fn new_count() -> Self {
// AggregateFunction::Count(i128::zero())
// }
//
// pub fn make_new(&self) -> Self {
// match self {
// AggregateFunction::Sum(_) => AggregateFunction::new_sum(),
// AggregateFunction::Min(_) => AggregateFunction::new_min(),
// AggregateFunction::Max(_) => AggregateFunction::new_max(),
// AggregateFunction::Avg(_, _) => AggregateFunction::new_avg(),
// AggregateFunction::Count(_) => AggregateFunction::new_count(),
// }
// }
//
// pub fn accumulate(&mut self, v: i128) {
// match self {
// AggregateFunction::Sum(s) => {
// s = *s + v;
// }
// AggregateFunction::Min(m) => {
// if v < *m {
// m = v;
// }
// }
// AggregateFunction::Max(m) => {
// if v > *m {
// m = v;
// }
// }
// AggregateFunction::Avg(s, c) => {
// s = *s + v;
// c = *c + 1;
// }
// AggregateFunction::Count(s) => {
// s = *s + 1;
// }
// }
// }
//
// pub fn result(&self) -> i128 {
// match self {
// AggregateFunction::Sum(s) => *s,
// AggregateFunction::Min(m) => *m,
// AggregateFunction::Max(m) => *m,
// AggregateFunction::Avg(s, c) => {
// println!("{s} {c}");
// let r = *s as f64 / *c as f64;
// (r * 10_i128.pow(DECIMAL_SCALE as u32) as f64) as i128
// }
// AggregateFunction::Count(s) => *s,
// }
// }
// pub fn reset(&mut self) {
// match self {
// AggregateFunction::Sum(s) => *s = i128::zero(),
// AggregateFunction::Min(m) => *m = i128::max_value(),
// AggregateFunction::Max(m) => *m = i128::min_value(),
// AggregateFunction::Avg(s, c) => {
// s = i128::zero();
// c = i128::zero();
// }
// AggregateFunction::Count(s) => *s = i128::zero(),
// }
// }
// }
