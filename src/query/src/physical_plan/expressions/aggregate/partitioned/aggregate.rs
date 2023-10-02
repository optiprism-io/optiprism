use std::collections::HashMap;
use std::marker::PhantomData;
use std::result;
use std::sync::Arc;

use ahash::RandomState;
use arrow::array::Array;
use arrow::array::ArrayRef;
use arrow::array::BooleanArray;
use arrow::array::Decimal128Array;
use arrow::array::Float32Array;
use arrow::array::Float32Builder;
use arrow::array::Float64Array;
use arrow::array::Float64Builder;
use arrow::array::Int16Array;
use arrow::array::Int16Builder;
use arrow::array::Int32Array;
use arrow::array::Int32Builder;
use arrow::array::Int64Array;
use arrow::array::Int64Builder;
use arrow::array::Int8Array;
use arrow::array::Int8Builder;
use arrow::array::UInt16Array;
use arrow::array::UInt16Builder;
use arrow::array::UInt32Array;
use arrow::array::UInt32Builder;
use arrow::array::UInt64Array;
use arrow::array::UInt64Builder;
use arrow::array::UInt8Array;
use arrow::array::UInt8Builder;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::record_batch::RecordBatch;
use arrow::row::Row;
use arrow::row::SortField;
use common::arrow::DecimalBuilder;
use common::DECIMAL_MULTIPLIER;
use common::DECIMAL_PRECISION;
use common::DECIMAL_SCALE;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::PhysicalExprRef;
use num_traits::Bounded;
use num_traits::Num;
use num_traits::NumCast;

use crate::error::Result;
use crate::physical_plan::expressions::aggregate::Groups;
use crate::physical_plan::expressions::aggregate::PartitionedAggregateExpr;
use crate::physical_plan::expressions::check_filter;
use crate::physical_plan::expressions::segmentation::aggregate::AggregateFunction;

#[derive(Debug)]
struct Group<IT, OT>
where
    IT: Copy + Num + Bounded + NumCast + PartialOrd + Clone + std::fmt::Display,
    OT: Copy + Num + Bounded + NumCast + PartialOrd + Clone + std::fmt::Display,
{
    inner_fn: AggregateFunction<IT>,
    outer_fn: AggregateFunction<OT>,
    first: bool,
    last_partition: i64,
}

impl<IT, OT> Group<IT, OT>
where
    IT: Copy + Num + Bounded + NumCast + PartialOrd + Clone + std::fmt::Display,
    OT: Copy + Num + Bounded + NumCast + PartialOrd + Clone + std::fmt::Display,
{
    pub fn new(inner_fn: AggregateFunction<IT>, outer_fn: AggregateFunction<OT>) -> Self {
        Self {
            inner_fn,
            outer_fn,
            first: true,
            last_partition: 0,
        }
    }
}

#[derive(Debug)]
pub struct Aggregate<T, IT, OT>
where
    T: Copy + Num + Bounded + NumCast + PartialOrd + Clone + std::fmt::Display,
    IT: Copy + Num + Bounded + NumCast + PartialOrd + Clone + std::fmt::Display,
    OT: Copy + Num + Bounded + NumCast + PartialOrd + Clone + std::fmt::Display,
{
    filter: Option<PhysicalExprRef>,
    predicate: Column,
    typ: PhantomData<T>,
    inner_fn: AggregateFunction<IT>,
    outer_fn: AggregateFunction<OT>,
    groups: Option<Groups<Group<IT, OT>>>,
    single_group: Group<IT, OT>,
    partition_col: Column,
    skip: bool,
    skip_partition: i64,
    t: PhantomData<T>,
}

impl<T, IT, OT> Aggregate<T, IT, OT>
where
    T: Copy + Num + Bounded + NumCast + PartialOrd + Clone + std::fmt::Display,
    IT: Copy + Num + Bounded + NumCast + PartialOrd + Clone + std::fmt::Display,
    OT: Copy + Num + Bounded + NumCast + PartialOrd + Clone + std::fmt::Display,
{
    pub fn try_new(
        filter: Option<PhysicalExprRef>,
        inner_fn: AggregateFunction<IT>,
        outer_fn: AggregateFunction<OT>,
        predicate: Column,
        groups: Option<Vec<(PhysicalExprRef, String, SortField)>>,
        partition_col: Column,
    ) -> Result<Self> {
        Ok(Self {
            filter,
            predicate,
            typ: Default::default(),
            inner_fn: inner_fn.make_new(),
            outer_fn: outer_fn.make_new(),
            groups: Groups::maybe_from(groups)?,
            single_group: Group::new(inner_fn, outer_fn),
            partition_col,
            skip: false,
            skip_partition: 0,
            t: Default::default(),
        })
    }
}

macro_rules! agg {
    ($ty:ident,$array_ty:ident,$inner_acc_ty:ident,$outer_acc_ty:ident,$b:ident,$mul:expr,$dt:expr) => {
        impl PartitionedAggregateExpr for Aggregate<$ty, $inner_acc_ty, $outer_acc_ty> {
            fn group_columns(&self) -> Vec<(PhysicalExprRef, String)> {
                if let Some(groups) = &self.groups {
                    groups
                        .exprs
                        .iter()
                        .zip(groups.names.iter())
                        .map(|(a, b)| (a.clone(), b.clone()))
                        .collect()
                } else {
                    vec![]
                }
            }

            fn fields(&self) -> Vec<Field> {
                let field = Field::new("partitioned_agg", $dt, true);
                println!("{:?}", field);
                vec![field]
            }

            fn evaluate(
                &mut self,
                batch: &RecordBatch,
                partition_exist: Option<&HashMap<i64, (), RandomState>>,
            ) -> crate::Result<()> {
                let filter = if self.filter.is_some() {
                    Some(
                        self.filter
                            .clone()
                            .unwrap()
                            .evaluate(batch)?
                            .into_array(batch.num_rows())
                            .as_any()
                            .downcast_ref::<BooleanArray>()
                            .unwrap()
                            .clone(),
                    )
                } else {
                    None
                };

                let rows = if let Some(groups) = &mut self.groups {
                    let arrs = groups
                        .exprs
                        .iter()
                        .map(|e| {
                            e.evaluate(batch)
                                .and_then(|v| Ok(v.into_array(batch.num_rows()).clone()))
                        })
                        .collect::<result::Result<Vec<_>, _>>()?;

                    Some(groups.row_converter.convert_columns(&arrs)?)
                } else {
                    None
                };

                let predicate = self
                    .predicate
                    .evaluate(batch)?
                    .into_array(batch.num_rows())
                    .as_any()
                    .downcast_ref::<$array_ty>()
                    .unwrap()
                    .clone();

                let partitions = self
                    .partition_col
                    .evaluate(batch)?
                    .into_array(batch.num_rows())
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .clone();

                for (row_id, partition) in partitions.into_iter().enumerate() {
                    let partition = partition.unwrap();
                    if self.skip {
                        if self.skip_partition != partition {
                            self.skip = false;
                        } else {
                            continue;
                        }
                    }
                    if let Some(exists) = partition_exist {
                        if !exists.contains_key(&partition) {
                            self.skip = true;
                            continue;
                        }
                    }

                    let bucket = if let Some(groups) = &mut self.groups {
                        groups
                            .groups
                            .entry(rows.as_ref().unwrap().row(row_id).owned())
                            .or_insert_with(|| {
                                let bucket =
                                    Group::new(self.inner_fn.make_new(), self.outer_fn.make_new());
                                bucket
                            })
                    } else {
                        &mut self.single_group
                    };

                    if bucket.first {
                        bucket.first = false;
                        bucket.last_partition = partition;
                    }

                    if let Some(filter) = &filter {
                        if !check_filter(filter, row_id) {
                            continue;
                        }
                    }

                    if bucket.last_partition != partition {
                        let v = bucket.inner_fn.result();
                        // for float multiply will be 1., which will be optimized in release mode
                        bucket
                            .outer_fn
                            .accumulate(v as $outer_acc_ty * $mul as $outer_acc_ty);
                        bucket.last_partition = partition;

                        bucket.inner_fn.reset();
                    }
                    bucket
                        .inner_fn
                        .accumulate(predicate.value(row_id) as $inner_acc_ty);
                }

                Ok(())
            }

            fn finalize(&mut self) -> Result<Vec<ArrayRef>> {
                if let Some(groups) = &mut self.groups {
                    let mut rows: Vec<Row> = Vec::with_capacity(groups.groups.len());
                    let mut res_col_b = $b::with_capacity(groups.groups.len());
                    for (row, group) in groups.groups.iter_mut() {
                        rows.push(row.row());
                        group
                            .outer_fn
                            .accumulate(group.inner_fn.result() as $outer_acc_ty * $mul);
                        let res = group.outer_fn.result();
                        res_col_b.append_value(res);
                    }

                    let group_col = groups.row_converter.convert_rows(rows)?;
                    let res_col = res_col_b.finish();
                    let res_col = Arc::new(res_col) as ArrayRef;
                    Ok(vec![group_col, vec![res_col]].concat())
                } else {
                    let mut res_col_b = $b::with_capacity(1);
                    self.single_group
                        .outer_fn
                        .accumulate(self.single_group.inner_fn.result() as $outer_acc_ty);
                    res_col_b.append_value(self.single_group.outer_fn.result());
                    let res_col = res_col_b.finish();
                    let res_col = Arc::new(res_col) as ArrayRef;
                    Ok(vec![res_col])
                }
            }

            fn make_new(&self) -> Result<Box<dyn PartitionedAggregateExpr>> {
                let groups = if let Some(groups) = &self.groups {
                    Some(groups.try_make_new()?)
                } else {
                    None
                };
                let c: Aggregate<$ty, $inner_acc_ty, $outer_acc_ty> = Aggregate {
                    filter: self.filter.clone(),
                    typ: Default::default(),
                    predicate: self.predicate.clone(),
                    inner_fn: self.inner_fn.make_new(),
                    outer_fn: self.outer_fn.make_new(),
                    groups,
                    partition_col: self.partition_col.clone(),
                    skip: false,
                    skip_partition: 0,
                    single_group: Group::new(self.inner_fn.make_new(), self.outer_fn.make_new()),
                    t: Default::default(),
                };

                Ok(Box::new(c))
            }
        }
    };
}

agg!(i8, Int8Array, i8, i8, Int8Builder, 1, DataType::Int8);
agg!(
    i8,
    Int8Array,
    i8,
    f64,
    Float64Builder,
    1.,
    DataType::Float64
);
agg!(i8, Int8Array, i8, i64, Int64Builder, 1, DataType::Int64);
agg!(
    i8,
    Int8Array,
    f64,
    f64,
    Float64Builder,
    1.,
    DataType::Float64
);
agg!(i8, Int8Array, f64, i64, Int64Builder, 1, DataType::Int64);
agg!(i8, Int8Array, i64, i64, Int64Builder, 1, DataType::Int64);
agg!(
    i8,
    Int8Array,
    i64,
    f64,
    Float64Builder,
    1.,
    DataType::Float64
);
agg!(
    i8,
    Int8Array,
    i64,
    i128,
    DecimalBuilder,
    DECIMAL_MULTIPLIER,
    DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE)
);
agg!(i16, Int16Array, i16, i16, Int16Builder, 1, DataType::Int16);
agg!(
    i16,
    Int16Array,
    i16,
    f64,
    Float64Builder,
    1.,
    DataType::Float64
);
agg!(i16, Int16Array, i16, i64, Int64Builder, 1, DataType::Int64);
agg!(
    i16,
    Int16Array,
    f64,
    f64,
    Float64Builder,
    1.,
    DataType::Float64
);
agg!(i16, Int16Array, f64, i64, Int64Builder, 1, DataType::Int64);
agg!(i16, Int16Array, i64, i64, Int64Builder, 1, DataType::Int64);
agg!(
    i16,
    Int16Array,
    i64,
    f64,
    Float64Builder,
    1.,
    DataType::Float64
);
agg!(
    i16,
    Int16Array,
    i64,
    i128,
    DecimalBuilder,
    DECIMAL_MULTIPLIER,
    DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE)
);
agg!(i32, Int32Array, i32, i32, Int32Builder, 1, DataType::Int32);
agg!(
    i32,
    Int32Array,
    i32,
    f64,
    Float64Builder,
    1.,
    DataType::Float64
);
agg!(i32, Int32Array, i32, i64, Int64Builder, 1, DataType::Int64);
agg!(
    i32,
    Int32Array,
    f64,
    f64,
    Float64Builder,
    1.,
    DataType::Float64
);
agg!(i32, Int32Array, f64, i64, Int64Builder, 1, DataType::Int64);
agg!(i32, Int32Array, i64, i64, Int64Builder, 1, DataType::Int64);
agg!(
    i32,
    Int32Array,
    i64,
    f64,
    Float64Builder,
    1.,
    DataType::Float64
);
agg!(
    i32,
    Int32Array,
    i64,
    i128,
    DecimalBuilder,
    DECIMAL_MULTIPLIER,
    DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE)
);
agg!(i64, Int64Array, i64, i64, Int64Builder, 1, DataType::Int64);
agg!(
    i64,
    Int64Array,
    i64,
    f64,
    Float64Builder,
    1.,
    DataType::Float64
);
agg!(
    i64,
    Int64Array,
    i64,
    i128,
    DecimalBuilder,
    DECIMAL_MULTIPLIER,
    DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE)
);
agg!(
    i64,
    Int64Array,
    f64,
    f64,
    Float64Builder,
    1.,
    DataType::Float64
);
agg!(i64, Int64Array, f64, i64, Int64Builder, 1, DataType::Int64);
agg!(
    i64,
    Int64Array,
    i128,
    i128,
    DecimalBuilder,
    DECIMAL_MULTIPLIER,
    DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE)
);
agg!(
    i64,
    Int64Array,
    i128,
    f64,
    Float64Builder,
    1.,
    DataType::Float64
);
agg!(i64, Int64Array, i128, i64, Int64Builder, 1, DataType::Int64);
agg!(u8, UInt8Array, u8, u8, UInt8Builder, 1, DataType::UInt8);
agg!(
    u8,
    UInt8Array,
    u8,
    f64,
    Float64Builder,
    1.,
    DataType::Float64
);
agg!(u8, UInt8Array, u8, u64, UInt64Builder, 1, DataType::UInt64);
agg!(u8, UInt8Array, u8, i64, Int64Builder, 1, DataType::Int64);
agg!(
    u8,
    UInt8Array,
    f64,
    f64,
    Float64Builder,
    1.,
    DataType::Float64
);
agg!(u8, UInt8Array, f64, i64, Int64Builder, 1, DataType::Int64);
agg!(u8, UInt8Array, u64, u64, UInt64Builder, 1, DataType::UInt64);
agg!(
    u8,
    UInt8Array,
    u64,
    f64,
    Float64Builder,
    1.,
    DataType::Float64
);
agg!(
    u8,
    UInt8Array,
    u64,
    i128,
    DecimalBuilder,
    DECIMAL_MULTIPLIER,
    DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE)
);
agg!(u8, UInt8Array, u64, i64, Int64Builder, 1, DataType::Int64);
agg!(u8, UInt8Array, i64, i64, Int64Builder, 1, DataType::Int64);
agg!(
    u8,
    UInt8Array,
    i64,
    f64,
    Float64Builder,
    1.,
    DataType::Float64
);
agg!(
    u8,
    UInt8Array,
    i64,
    i128,
    DecimalBuilder,
    DECIMAL_MULTIPLIER,
    DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE)
);
agg!(
    u16,
    UInt16Array,
    u16,
    u16,
    UInt16Builder,
    1,
    DataType::UInt16
);
agg!(
    u16,
    UInt16Array,
    u16,
    f64,
    Float64Builder,
    1.,
    DataType::Float64
);
agg!(
    u16,
    UInt16Array,
    u16,
    u64,
    UInt64Builder,
    1,
    DataType::UInt64
);
agg!(u16, UInt16Array, u16, i64, Int64Builder, 1, DataType::Int64);
agg!(
    u16,
    UInt16Array,
    f64,
    f64,
    Float64Builder,
    1.,
    DataType::Float64
);
agg!(u16, UInt16Array, f64, i64, Int64Builder, 1, DataType::Int64);
agg!(
    u16,
    UInt16Array,
    u64,
    u64,
    UInt64Builder,
    1,
    DataType::UInt64
);
agg!(
    u16,
    UInt16Array,
    u64,
    f64,
    Float64Builder,
    1.,
    DataType::Float64
);
agg!(
    u16,
    UInt16Array,
    u64,
    i128,
    DecimalBuilder,
    DECIMAL_MULTIPLIER,
    DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE)
);
agg!(u16, UInt16Array, u64, i64, Int64Builder, 1, DataType::Int64);
agg!(u16, UInt16Array, i64, i64, Int64Builder, 1, DataType::Int64);
agg!(
    u16,
    UInt16Array,
    i64,
    f64,
    Float64Builder,
    1.,
    DataType::Float64
);
agg!(
    u16,
    UInt16Array,
    i64,
    i128,
    DecimalBuilder,
    DECIMAL_MULTIPLIER,
    DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE)
);
agg!(
    u32,
    UInt32Array,
    u32,
    u32,
    UInt32Builder,
    1,
    DataType::UInt32
);
agg!(
    u32,
    UInt32Array,
    u32,
    f64,
    Float64Builder,
    1.,
    DataType::Float64
);
agg!(
    u32,
    UInt32Array,
    u32,
    u64,
    UInt64Builder,
    1,
    DataType::UInt64
);
agg!(u32, UInt32Array, u32, i64, Int64Builder, 1, DataType::Int64);
agg!(
    u32,
    UInt32Array,
    f64,
    f64,
    Float64Builder,
    1.,
    DataType::Float64
);
agg!(u32, UInt32Array, f64, i64, Int64Builder, 1, DataType::Int64);
agg!(
    u32,
    UInt32Array,
    u64,
    u64,
    UInt64Builder,
    1,
    DataType::UInt64
);
agg!(
    u32,
    UInt32Array,
    u64,
    f64,
    Float64Builder,
    1.,
    DataType::Float64
);
agg!(
    u32,
    UInt32Array,
    u64,
    i128,
    DecimalBuilder,
    DECIMAL_MULTIPLIER,
    DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE)
);
agg!(u32, UInt32Array, u64, i64, Int64Builder, 1, DataType::Int64);
agg!(u32, UInt32Array, i64, i64, Int64Builder, 1, DataType::Int64);
agg!(
    u32,
    UInt32Array,
    i64,
    f64,
    Float64Builder,
    1.,
    DataType::Float64
);
agg!(
    u32,
    UInt32Array,
    i64,
    i128,
    DecimalBuilder,
    DECIMAL_MULTIPLIER,
    DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE)
);
agg!(
    u64,
    UInt64Array,
    u64,
    u64,
    UInt64Builder,
    1,
    DataType::UInt64
);
agg!(
    u64,
    UInt64Array,
    u64,
    f64,
    Float64Builder,
    1.,
    DataType::Float64
);
agg!(
    u64,
    UInt64Array,
    u64,
    i128,
    DecimalBuilder,
    DECIMAL_MULTIPLIER,
    DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE)
);
agg!(u64, UInt64Array, u64, i64, Int64Builder, 1, DataType::Int64);
agg!(
    u64,
    UInt64Array,
    f64,
    f64,
    Float64Builder,
    1.,
    DataType::Float64
);
agg!(u64, UInt64Array, f64, i64, Int64Builder, 1, DataType::Int64);
agg!(
    u64,
    UInt64Array,
    i128,
    i128,
    DecimalBuilder,
    DECIMAL_MULTIPLIER,
    DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE)
);
agg!(
    u64,
    UInt64Array,
    u128,
    f64,
    Float64Builder,
    1.,
    DataType::Float64
);
agg!(
    u64,
    UInt64Array,
    u128,
    i128,
    DecimalBuilder,
    DECIMAL_MULTIPLIER,
    DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE)
);
agg!(
    u64,
    UInt64Array,
    u128,
    i64,
    Int64Builder,
    1,
    DataType::Int64
);
agg!(u64, UInt64Array, i64, i64, Int64Builder, 1, DataType::Int64);
agg!(
    u64,
    UInt64Array,
    i64,
    f64,
    Float64Builder,
    1.,
    DataType::Float64
);
agg!(
    u64,
    UInt64Array,
    i64,
    i128,
    DecimalBuilder,
    DECIMAL_MULTIPLIER,
    DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE)
);
agg!(
    f32,
    Float32Array,
    f32,
    f32,
    Float32Builder,
    1.,
    DataType::Float32
);
agg!(
    f32,
    Float32Array,
    f32,
    f64,
    Float64Builder,
    1.,
    DataType::Float64
);
agg!(
    f32,
    Float32Array,
    f32,
    i64,
    Int64Builder,
    1,
    DataType::Int64
);
agg!(
    f32,
    Float32Array,
    f64,
    f64,
    Float64Builder,
    1.,
    DataType::Float64
);
agg!(
    f32,
    Float32Array,
    f64,
    i64,
    Int64Builder,
    1,
    DataType::Int64
);
agg!(
    f32,
    Float32Array,
    i64,
    i64,
    Int64Builder,
    1,
    DataType::Int64
);
agg!(
    f32,
    Float32Array,
    i64,
    f64,
    Float64Builder,
    1.,
    DataType::Float64
);
agg!(
    f32,
    Float32Array,
    i64,
    i128,
    DecimalBuilder,
    DECIMAL_MULTIPLIER,
    DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE)
);
agg!(
    f64,
    Float64Array,
    f64,
    f64,
    Float64Builder,
    1.,
    DataType::Float64
);
agg!(
    f64,
    Float64Array,
    f64,
    i64,
    Int64Builder,
    1,
    DataType::Int64
);
agg!(
    f64,
    Float64Array,
    i64,
    i64,
    Int64Builder,
    1,
    DataType::Int64
);
agg!(
    f64,
    Float64Array,
    i64,
    f64,
    Float64Builder,
    1.,
    DataType::Float64
);
agg!(
    f64,
    Float64Array,
    i64,
    i128,
    DecimalBuilder,
    DECIMAL_MULTIPLIER,
    DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE)
);
agg!(
    i128,
    Decimal128Array,
    i128,
    i128,
    DecimalBuilder,
    1,
    DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE)
);
agg!(
    i128,
    Decimal128Array,
    i128,
    f64,
    Float64Builder,
    1. / DECIMAL_MULTIPLIER as f64,
    DataType::Float64
);
agg!(
    i128,
    Decimal128Array,
    i128,
    i64,
    Int64Builder,
    1,
    DataType::Int64
);
agg!(
    i128,
    Decimal128Array,
    f64,
    f64,
    Float64Builder,
    1. / DECIMAL_MULTIPLIER as f64,
    DataType::Float64
);
agg!(
    i128,
    Decimal128Array,
    f64,
    i64,
    Int64Builder,
    1,
    DataType::Int64
);
agg!(
    i128,
    Decimal128Array,
    i64,
    i64,
    Int64Builder,
    1,
    DataType::Int64
);
agg!(
    i128,
    Decimal128Array,
    i64,
    f64,
    Float64Builder,
    1.,
    DataType::Float64
);
agg!(
    i128,
    Decimal128Array,
    i64,
    i128,
    DecimalBuilder,
    DECIMAL_MULTIPLIER,
    DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE)
);

// agg_inner_decimal!(i8, Int8Array, f64, Float64Builder, Float64);
// agg_decimal_decimal!(i8, Int8Array);
// agg_outer_float!(i8, Int8Array, i64);
//
// agg!(i16, Int8Array, i16, i16, Int16Builder, Int16);
// agg!(i16, Int8Array, i64, i64, Int64Builder, Int64);
// agg_inner_decimal!(i16, Int16Array, f64, Float64Builder, Float64);
// // agg_outer_float!(i16, Int8Array, i64);
//
// agg!(i32, Int8Array, i32, i32, Int32Builder, Int32);
// agg!(i32, Int8Array, i64, i64, Int64Builder, Int64);
// agg_inner_decimal!(i32, Int32Array, f64, Float64Builder, Float64);
// // agg_outer_float!(i32, Int8Array, i64);
//
// agg!(i64, Int8Array, i64, i64, Int64Builder, Int64);
// agg_inner_decimal!(i64, Int64Array, f64, Float64Builder, Float64);
// agg_outer_float!(i32, Int8Array, i64);

// decimal!(i128);
// decimal!(f64);

// float!(f64);

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use datafusion::physical_expr::expressions::Column;
    use store::test_util::parse_markdown_tables;

    use crate::physical_plan::expressions::aggregate::partitioned::aggregate::Aggregate;
    use crate::physical_plan::expressions::aggregate::PartitionedAggregateExpr;
    use crate::physical_plan::expressions::segmentation::aggregate::AggregateFunction;

    #[test]
    fn count_min() {
        let data = r#"
| user_id(i64) | v(i8) |
|--------------|-------|
| 0            | 127     |
| 1            | 127     |
| 1            | 127     |
"#;
        let res = parse_markdown_tables(data).unwrap();
        let schema = res[0].schema().clone();
        let hash = HashMap::from_iter([(0, ()), (1, ()), (4, ())]);
        {
            let mut agg = Aggregate::<i8, i8, i8>::try_new(
                None,
                AggregateFunction::new_min(),
                AggregateFunction::new_max(),
                Column::new_with_schema("v", &schema).unwrap(),
                None,
                Column::new_with_schema("user_id", &schema).unwrap(),
            )
            .unwrap();
            for b in &res {
                agg.evaluate(b, Some(&hash)).unwrap();
            }

            let ar = agg.finalize().unwrap();
            println!("{:?}", ar);
        }
        {
            let mut agg = Aggregate::<i8, i8, f64>::try_new(
                None,
                AggregateFunction::new_min(),
                AggregateFunction::new_avg(),
                Column::new_with_schema("v", &schema).unwrap(),
                None,
                Column::new_with_schema("user_id", &schema).unwrap(),
            )
            .unwrap();
            for b in &res {
                agg.evaluate(b, Some(&hash)).unwrap();
            }

            let ar = agg.finalize().unwrap();
            println!("{:?}", ar);
        }
        {
            let mut agg = Aggregate::<i8, i64, f64>::try_new(
                None,
                AggregateFunction::new_sum(),
                AggregateFunction::new_avg(),
                Column::new_with_schema("v", &schema).unwrap(),
                None,
                Column::new_with_schema("user_id", &schema).unwrap(),
            )
            .unwrap();
            for b in &res {
                agg.evaluate(b, Some(&hash)).unwrap();
            }

            let ar = agg.finalize().unwrap();
            println!("{:?}", ar);
        }

        {
            let mut agg = Aggregate::<i8, i8, f64>::try_new(
                None,
                AggregateFunction::new_min(),
                AggregateFunction::new_avg(),
                Column::new_with_schema("v", &schema).unwrap(),
                None,
                Column::new_with_schema("user_id", &schema).unwrap(),
            )
            .unwrap();
            for b in &res {
                agg.evaluate(b, Some(&hash)).unwrap();
            }

            let ar = agg.finalize().unwrap();
            println!("{:?}", ar);
        }
    }
}
