use std::marker::PhantomData;
use std::sync::Mutex;

use arrow::array::Array;
use arrow::array::ArrayBuilder;
use arrow::array::BooleanArray;
use arrow::array::Decimal128Array;
use arrow::array::Float32Array;
use arrow::array::Float64Array;
use arrow::array::Int16Array;
use arrow::array::Int32Array;
use arrow::array::Int64Array;
use arrow::array::Int64Builder;
use arrow::array::Int8Array;
use arrow::array::TimestampMillisecondArray;
use arrow::array::UInt16Array;
use arrow::array::UInt32Array;
use arrow::array::UInt64Array;
use arrow::array::UInt8Array;
use arrow::buffer::ScalarBuffer;
use arrow::record_batch::RecordBatch;
use chrono::Duration;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::PhysicalExprRef;
use num_traits::Bounded;
use num_traits::Num;
use num_traits::NumCast;

use crate::error::Result;
use crate::physical_plan::expressions::check_filter;
use crate::physical_plan::expressions::segmentation::boolean_op::ComparisonOp;
use crate::physical_plan::expressions::segmentation::time_range::TimeRange;
use crate::physical_plan::expressions::segmentation::SegmentExpr;

#[derive(Debug, Clone)]
pub enum AggregateFunction<T>
where
    T: Copy + Num + Bounded + NumCast + PartialOrd + Clone + std::fmt::Display,
{
    Sum(T),
    Min(T),
    Max(T),
    Avg(T, T),
    Count(T),
}

impl<T> AggregateFunction<T>
where
    T: Copy + Num + Bounded + NumCast + PartialOrd + Clone + std::fmt::Display,
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

    pub fn make_new(&self) -> Self {
        match self {
            AggregateFunction::Sum(_) => AggregateFunction::new_sum(),
            AggregateFunction::Min(_) => AggregateFunction::new_min(),
            AggregateFunction::Max(_) => AggregateFunction::new_max(),
            AggregateFunction::Avg(_, _) => AggregateFunction::new_avg(),
            AggregateFunction::Count(_) => AggregateFunction::new_count(),
        }
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

    pub fn op(&self) -> &str {
        match self {
            AggregateFunction::Sum(_) => "sum",
            AggregateFunction::Min(_) => "min",
            AggregateFunction::Max(_) => "max",
            AggregateFunction::Avg(_, _) => "avg",
            AggregateFunction::Count(_) => "count",
        }
    }
}

#[derive(Debug)]
struct Inner<T>
where
    T: Copy + Num + Bounded + NumCast + PartialOrd + Clone + std::fmt::Display,
{
    agg: AggregateFunction<T>,
    last_partition: i64,
    res: Int64Builder,
    first: bool,
}

#[derive(Debug)]
pub struct Aggregate<T, OT, Op>
where
    OT: Copy + Num + Bounded + NumCast + PartialOrd + Clone + std::fmt::Display,
{
    filter: PhysicalExprRef,
    predicate: Column,
    ts_col: Column,
    partition_col: Column,
    inner: Mutex<Inner<OT>>,
    time_range: TimeRange,
    op: PhantomData<Op>,
    right: OT,
    typ: PhantomData<T>,
}
#[allow(clippy::too_many_arguments)]
impl<T, OT, Op> Aggregate<T, OT, Op>
where
    OT: Copy + Num + Bounded + NumCast + PartialOrd + Clone + std::fmt::Display,
{
    pub fn new(
        filter: PhysicalExprRef,
        ts_col: Column,
        partition_col: Column,
        predicate: Column,
        agg: AggregateFunction<OT>,
        right: OT,
        time_range: TimeRange,
    ) -> Self {
        let inner = Inner {
            agg,
            last_partition: 0,
            res: Int64Builder::with_capacity(1000),
            first: true,
        };
        Self {
            filter,
            predicate,
            ts_col,
            partition_col,
            inner: Mutex::new(inner),
            time_range,
            op: Default::default(),
            right,
            typ: Default::default(),
        }
    }
}

macro_rules! agg {
    ($ty:ty,$array_ty:ident,$acc_ty:ty) => {
        impl<Op> SegmentExpr for Aggregate<$ty, $acc_ty, Op>
        where Op: ComparisonOp<$acc_ty>
        {
            fn evaluate(
                &self,
                batch: &RecordBatch,
            ) -> Result<()> {
                let ts = self
                    .ts_col
                    .evaluate(batch)?
                    .into_array(batch.num_rows())?
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .unwrap()
                    .clone();

                let filter = self
                    .filter
                    .evaluate(batch)?
                    .into_array(batch.num_rows())?
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .clone();

                let predicate = self
                    .predicate
                    .evaluate(batch)?
                    .into_array(batch.num_rows())?
                    .as_any()
                    .downcast_ref::<$array_ty>()
                    .unwrap()
                    .clone();

                    let mut inner = self.inner.lock().unwrap();
                    let partition = self.partition_col.evaluate(batch)?.into_array(batch.num_rows())?
                                    .as_any()
                                    .downcast_ref::<Int64Array>()
                                    .unwrap()
                                    .clone();
                for (row_id, partition) in partition.into_iter().enumerate() {
                    if inner.first {
                        inner.first = false;
                        inner.last_partition = partition.unwrap();
                    }
                    if !self.time_range.check_bounds(ts.value(row_id)) {
                        continue;
                    }

                    if !check_filter(&filter, row_id) {
                        continue;
                    }

                    if inner.last_partition != partition.unwrap() {
                        let res = Op::perform(inner.agg.result(), self.right);

                        if !res {
                            inner.res.append_null();
                        } else {
                            let p = inner.last_partition;
                            inner.res.append_value(p);
                        }

                        inner.agg.reset();
                        inner.last_partition = partition.unwrap();
                    }
                    inner.agg.accumulate(predicate.value(row_id) as $acc_ty);
                }
                Ok(())
            }

            fn finalize(&self) -> Result<Int64Array> {
                let mut inner = self.inner.lock().unwrap();
                let res = Op::perform(inner.agg.result(), self.right);
                if !res {
                    inner.res.append_null();
                } else {
                    let v = inner.last_partition;
                    inner.res.append_value(v);
                }

                Ok(inner.res.finish())
            }
        }
    };
}

agg!(i8, Int8Array, i64);
agg!(i16, Int16Array, i64);
agg!(i32, Int32Array, i64);
agg!(i64, Int64Array, i128);
agg!(i128, Decimal128Array, i128);
agg!(u8, UInt8Array, i64);
agg!(u16, UInt16Array, i64);
agg!(u32, UInt32Array, i64);
agg!(u64, UInt64Array, i128);
agg!(u128, Decimal128Array, i128);
agg!(f32, Float32Array, f64);
agg!(f64, Float64Array, f64);
agg!(Decimal128Array, Decimal128Array, i128);

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use std::sync::Arc;

    use arrow::array::Array;
    use arrow::array::Int64Array;
    use arrow::record_batch::RecordBatch;
    use datafusion::physical_expr::expressions::BinaryExpr;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::expressions::Literal;
    use datafusion::physical_expr::PhysicalExprRef;
    use datafusion_common::ScalarValue;
    use datafusion_expr::Operator;
    use rust_decimal::Decimal;
    use common::DECIMAL_SCALE;
    use storage::test_util::parse_markdown_tables;

    use crate::physical_plan::expressions::segmentation::aggregate::Aggregate;
    use crate::physical_plan::expressions::segmentation::aggregate::AggregateFunction;
    use crate::physical_plan::expressions::segmentation::boolean_op;
    use crate::physical_plan::expressions::segmentation::boolean_op::Gt;
    use crate::physical_plan::expressions::segmentation::time_range::TimeRange;
    use crate::physical_plan::expressions::segmentation::SegmentExpr;

    #[test]
    fn test_int() {
        let data = r#"
    | user_id(i64) | ts(ts) | event(utf8) | v(i64) |
    |--------------|--------|-------------|--------|
    | 0            | 1      | e1          | 1      |
    | 0            | 2      | e2          | 1      |
    | 0            | 3      | e3          | 1      |
    | 0            | 4      | e1          | 0      |
    | 0            | 5      | e1          | 1      |
    | 0            | 6      | e2          | 1      |
    | 0            | 7      | e3          | 1      |
    |||||
    | 1            | 8      | e1          | 1      |
    | 1            | 9      | e3          | 2      |
    | 1            | 10     | e1          | 3      |
    | 1            | 11     | e2          | 4      |
    |||||
    | 2            | 12     | e2          | 1      |
    | 2            | 13     | e1          | 2      |
    | 2            | 14     | e2          | 3      |
"#;

        let mut res = parse_markdown_tables(data).unwrap();
        res = res
            .iter()
            .enumerate()
            .map(|(id, batch)| {
                let mut schema = (*batch.schema()).to_owned();
                schema.metadata.insert("id".to_string(), id.to_string());
                RecordBatch::try_new(Arc::new(schema.clone()), batch.columns().to_owned()).unwrap()
            })
            .collect::<Vec<_>>();

        let schema = res[0].schema();
        {
            let left = Arc::new(Column::new_with_schema("event", &schema).unwrap());
            let right = Arc::new(Literal::new(ScalarValue::Utf8(Some("e1".to_string()))));
            let f = BinaryExpr::new(left, Operator::Eq, right);
            let agg = Aggregate::<i64, i128, Gt>::new(
                Arc::new(f) as PhysicalExprRef,
                Column::new_with_schema("ts", &schema).unwrap(),
                Column::new_with_schema("user_id", &res[0].schema()).unwrap(),
                Column::new_with_schema("v", &schema).unwrap(),
                AggregateFunction::new_sum(),
                3,
                TimeRange::None,
            );

            for b in res {
                let _res = agg.evaluate(&b).unwrap();
            }
            let res = agg.finalize().unwrap();
            let exp = Int64Array::from(vec![None, Some(1), None]);
            assert_eq!(res, exp);
        }
    }

    #[test]
    fn test_decimal() {
        let data = r#"
| user_id(i64) | ts(ts) | event(utf8) | v(decimal) |
|--------------|--------|-------------|--------|
| 0            | 1      | e1          | 0.5   |
| 0            | 1      | e1          | 0.5   |
| 1            | 8      | e1          | 4.33   |
| 1            | 8      | e1          | 0.15   |
"#;

        let mut res = parse_markdown_tables(data).unwrap();
        res = res
            .iter()
            .enumerate()
            .map(|(id, batch)| {
                let mut schema = (*batch.schema()).to_owned();
                schema.metadata.insert("id".to_string(), id.to_string());
                RecordBatch::try_new(Arc::new(schema.clone()), batch.columns().to_owned()).unwrap()
            })
            .collect::<Vec<_>>();

        let schema = res[0].schema();
        {
            let left = Arc::new(Column::new_with_schema("event", &schema).unwrap());
            let right = Arc::new(Literal::new(ScalarValue::Utf8(Some("e1".to_string()))));
            let f = BinaryExpr::new(left, Operator::Eq, right);
            let mut right = Decimal::from_str("448").unwrap();
            right.rescale(DECIMAL_SCALE as u32);
            let agg = Aggregate::<i128, i128, boolean_op::Eq>::new(
                Arc::new(f) as PhysicalExprRef,
                Column::new_with_schema("ts", &schema).unwrap(),
                Column::new_with_schema("user_id", &schema).unwrap(),
                Column::new_with_schema("v", &schema).unwrap(),
                AggregateFunction::new_sum(),
                right.mantissa(),
                TimeRange::None,
            );

            for b in res {
                agg.evaluate(&b).unwrap();
            }
            let res = agg.finalize().unwrap();

            let exp = Int64Array::from(vec![None, Some(1)]);
            assert_eq!(res, exp);
        }
    }
}
