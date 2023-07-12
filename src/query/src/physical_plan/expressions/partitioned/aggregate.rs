use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::Mutex;

use arrow::array::Array;
use arrow::array::ArrayBuilder;
use arrow::array::ArrayRef;
use arrow::array::BooleanArray;
use arrow::array::BooleanBuilder;
use arrow::array::Decimal128Array;
use arrow::array::Decimal128Builder;
use arrow::array::Float16Array;
use arrow::array::Float32Array;
use arrow::array::Float32Builder;
use arrow::array::Float64Array;
use arrow::array::Float64Builder;
use arrow::array::Int16Array;
use arrow::array::Int32Array;
use arrow::array::Int64Array;
use arrow::array::Int64Builder;
use arrow::array::Int8Array;
use arrow::array::PrimitiveBuilder;
use arrow::array::TimestampMillisecondArray;
use arrow::array::UInt16Array;
use arrow::array::UInt32Array;
use arrow::array::UInt64Array;
use arrow::array::UInt8Array;
use arrow::record_batch::RecordBatch;
use arrow2::types::f16;
use chrono::Duration;
use common::DECIMAL_PRECISION;
use common::DECIMAL_SCALE;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::PhysicalExprRef;
use num_traits::Bounded;
use num_traits::Num;
use num_traits::NumCast;
use num_traits::Zero;

use crate::error::Result;
use crate::physical_plan::expressions::partitioned::boolean_op::ComparisonOp;
use crate::physical_plan::expressions::partitioned::boolean_op::Operator;
use crate::physical_plan::expressions::partitioned::check_filter;
use crate::physical_plan::expressions::partitioned::time_range::TimeRange;
use crate::physical_plan::expressions::partitioned::AggregateFunction;
use crate::physical_plan::expressions::partitioned::SegmentationExpr;

#[derive(Debug)]
struct AggregateInner<OT>
where OT: Copy + Num + Bounded + NumCast + PartialOrd + Clone
{
    last_hash: u64,
    out: BooleanBuilder,
    agg: AggregateFunction<OT>,
    last_ts: i64,
    val: OT,
    skip: bool,
}

#[derive(Debug)]
pub struct Aggregate<T, OT, Op>
where OT: Copy + Num + Bounded + NumCast + PartialOrd + Clone
{
    inner: Arc<Mutex<AggregateInner<OT>>>,
    filter: PhysicalExprRef,
    predicate: Column,
    ts_col: Column,
    time_range: TimeRange,
    op: PhantomData<Op>,
    right: OT,
    typ: PhantomData<T>,
    time_window: i64,
}

impl<T, OT, Op> Aggregate<T, OT, Op>
where OT: Copy + Num + Bounded + NumCast + PartialOrd + Clone
{
    pub fn try_new(
        filter: PhysicalExprRef,
        predicate: Column,
        agg: AggregateFunction<OT>,
        ts_col: Column,
        right: OT,
        time_range: TimeRange,
        time_window: Option<i64>,
    ) -> Result<Self> {
        let inner = AggregateInner {
            last_hash: 0,
            out: BooleanBuilder::with_capacity(10_000),
            agg,
            last_ts: 0,
            val: OT::zero(),
            skip: false,
        };

        Ok(Self {
            filter,
            inner: Arc::new(Mutex::new(inner)),
            predicate,
            ts_col,
            time_range,
            op: Default::default(),
            right,
            typ: Default::default(),
            time_window: time_window
                .map(|t| t)
                .unwrap_or(Duration::days(365).num_milliseconds()),
        })
    }
}

macro_rules! gen_agg_primitive {
    ($ty:ty,$array_ty:ident,$acc_ty:ty) => {
        impl<Op> SegmentationExpr for Aggregate<$ty, $acc_ty, Op>
        where Op: ComparisonOp<$acc_ty>
        {
            fn evaluate(
                &self,
                record_batch: &RecordBatch,
                hashes: &[u64],
            ) -> Result<Option<BooleanArray>> {
                let ts = self
                    .ts_col
                    .evaluate(record_batch)?
                    .into_array(record_batch.num_rows())
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .unwrap()
                    .clone();

                let to_filter = self
                    .filter
                    .evaluate(record_batch)?
                    .into_array(record_batch.num_rows())
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .clone();
                let mut inner = self.inner.lock().unwrap();
                let arr = self
                    .predicate
                    .evaluate(record_batch)?
                    .into_array(record_batch.num_rows())
                    .as_any()
                    .downcast_ref::<$array_ty>()
                    .unwrap()
                    .clone();
                for (idx, hash) in hashes.iter().enumerate() {
                    if inner.last_hash == 0 {
                        inner.last_hash = *hash;
                        inner.last_ts = ts.value(idx);
                    }
                    if !self.time_range.check_bounds(ts.value(idx)) {
                        continue;
                    }
                    if check_filter(&to_filter, idx) == false {
                        continue;
                    }
                    if *hash != inner.last_hash {
                        let val = inner.vvakal;
                        inner.last_hash = *hash;
                        inner.val = <$acc_ty>::zero();
                        inner.agg.reset();
                        inner.last_ts = ts.value(idx);
                        let skip = inner.skip;
                        inner.skip = false;
                        if !skip {
                            let res = Op::perform(val, self.right);
                            if !res {
                                inner.out.append_value(false);
                                inner.val = inner.agg.accumulate(arr.value(idx).into());
                                continue;
                            }

                            inner.out.append_value(true);
                        }
                    } else if !inner.skip && ts.value(idx) - inner.last_ts >= self.time_window {
                        let val = inner.val;
                        inner.val = <$acc_ty>::zero();
                        inner.agg.reset();
                        let res = Op::perform(val, self.right);
                        if !res {
                            inner.out.append_value(false);
                            inner.skip = true;
                        } else {
                            inner.last_ts = ts.value(idx);
                        }
                    } else if inner.skip {
                        continue;
                    }
                    inner.val = inner.agg.accumulate(arr.value(idx).into());
                }

                if inner.out.len() > 0 {
                    Ok(Some(inner.out.finish()))
                } else {
                    Ok(None)
                }
            }

            fn finalize(&self) -> Result<BooleanArray> {
                let mut inner = self.inner.lock().unwrap();
                if inner.skip {
                    inner.out.append_value(false);
                    return Ok(inner.out.finish());
                }

                let val = inner.val;
                let res = Op::perform(val, self.right);

                if !res {
                    inner.out.append_value(false);
                } else {
                    inner.out.append_value(true);
                }
                Ok(inner.out.finish())
            }
        }
    };
}

gen_agg_primitive!(i8, Int8Array, i64);
gen_agg_primitive!(i16, Int16Array, i64);
gen_agg_primitive!(i32, Int32Array, i64);
gen_agg_primitive!(i64, Int64Array, i128);
gen_agg_primitive!(i128, Decimal128Array, i128);
gen_agg_primitive!(u8, UInt8Array, i64);
gen_agg_primitive!(u16, UInt16Array, i64);
gen_agg_primitive!(u32, UInt32Array, i64);
gen_agg_primitive!(u64, UInt64Array, i128);
gen_agg_primitive!(u128, Decimal128Array, i128);
gen_agg_primitive!(f32, Float32Array, f64);
gen_agg_primitive!(f64, Float64Array, f64);
gen_agg_primitive!(Decimal128Array, Decimal128Array, i128);
// todo add decimal 256
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Array;
    use arrow::array::ArrayRef;
    use arrow::array::BooleanArray;
    use arrow::array::Int64Array;
    use arrow::array::TimestampMillisecondArray;
    use arrow::datatypes::DataType;
    use arrow::datatypes::Field;
    use arrow::datatypes::Schema;
    use arrow::datatypes::SchemaRef;
    use arrow::datatypes::TimeUnit;
    use arrow::record_batch::RecordBatch;
    use chrono::Duration;
    use datafusion::physical_expr::expressions;
    use datafusion::physical_expr::expressions::BinaryExpr;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::expressions::Literal;
    use datafusion::physical_expr::hash_utils::create_hashes;
    use datafusion::physical_expr::PhysicalExprRef;
    use datafusion_common::ScalarValue;
    use datafusion_expr::binary_expr;
    use datafusion_expr::lit;
    use datafusion_expr::Expr;
    use datafusion_expr::Operator;
    use store::test_util::parse_markdown_table_v1;

    use crate::physical_plan::expressions::partitioned::aggregate::Aggregate;
    use crate::physical_plan::expressions::partitioned::boolean_op::Gt;
    use crate::physical_plan::expressions::partitioned::count::Count;
    use crate::physical_plan::expressions::partitioned::time_range::TimeRange;
    use crate::physical_plan::expressions::partitioned::AggregateFunction;
    use crate::physical_plan::expressions::partitioned::SegmentationExpr;

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

| 1            | 8      | e1          | 1      |
| 1            | 9      | e3          | 2      |
| 1            | 10     | e1          | 3      |
| 1            | 11     | e2          | 4      |

| 2            | 12     | e2          | 1      |
| 2            | 13     | e1          | 2      |
| 2            | 14     | e2          | 3      |
"#;

        let res = parse_markdown_table_v1(data).unwrap();

        let mut random_state = ahash::RandomState::with_seeds(0, 0, 0, 0);
        let mut hash_buf = vec![];
        hash_buf.resize(res.num_rows(), 0);
        create_hashes(
            &vec![res.columns()[0].clone()],
            &mut random_state,
            &mut hash_buf,
        )
        .unwrap();

        {
            let left = Arc::new(Column::new_with_schema("event", &res.schema()).unwrap());
            let right = Arc::new(Literal::new(ScalarValue::Utf8(Some("e1".to_string()))));
            let f = BinaryExpr::new(left, Operator::Eq, right);
            let mut agg = Aggregate::<i64, i128, Gt>::try_new(
                Arc::new(f) as PhysicalExprRef,
                Column::new_with_schema("v", &res.schema()).unwrap(),
                AggregateFunction::new_sum(),
                Column::new_with_schema("ts", &res.schema()).unwrap(),
                2,
                TimeRange::None,
                None,
            )
            .unwrap();
            let res = agg.evaluate(&res, &hash_buf).unwrap();
            let right = BooleanArray::from(vec![false, true]);
            assert_eq!(res, Some(right));

            let res = agg.finalize().unwrap();
            let right = BooleanArray::from(vec![false]);
            assert_eq!(res, right);
        }
    }

    #[test]
    fn test_window() {
        let data = r#"
| user_id(i64) | ts(ts) | event(utf8) | v(i64) |
|--------------|--------|-------------|--------|
| 0            | 0      | e1          | 1      |
| 0            | 1      | e1          | 1      |
| 0            | 2      | e1          | 1      |
| 0            | 3      | e1          | 1      |

| 1            | 11     | e1          | 1      |
| 1            | 12     | e1          | 1      |
| 1            | 13     | e1          | 1      |
| 1            | 14     | e1          | 1      |

| 2            | 16     | e1          | 1      |
| 2            | 17     | e1          | 1      |
| 2            | 18     | e1          | 1      |

| 3            | 19     | e1          | 1      |
| 3            | 20     | e1          | 1      |
| 3            | 22     | e1          | 1      |
| 3            | 23     | e1          | 1      |
| 3            | 24     | e1          | 1      |
| 3            | 24     | e1          | 1      |
"#;

        let res = parse_markdown_table_v1(data).unwrap();

        let mut random_state = ahash::RandomState::with_seeds(0, 0, 0, 0);
        let mut hash_buf = vec![];
        hash_buf.resize(res.num_rows(), 0);
        create_hashes(
            &vec![res.columns()[0].clone()],
            &mut random_state,
            &mut hash_buf,
        )
        .unwrap();

        {
            let left = Arc::new(Column::new_with_schema("event", &res.schema()).unwrap());
            let right = Arc::new(Literal::new(ScalarValue::Utf8(Some("e1".to_string()))));
            let f = BinaryExpr::new(left, Operator::Eq, right);
            let mut agg = Aggregate::<i64, i128, Gt>::try_new(
                Arc::new(f) as PhysicalExprRef,
                Column::new_with_schema("v", &res.schema()).unwrap(),
                AggregateFunction::new_sum(),
                Column::new_with_schema("ts", &res.schema()).unwrap(),
                1,
                TimeRange::None,
                Some(Duration::nanoseconds(2).num_nanoseconds().unwrap()),
            )
            .unwrap();
            let res = agg.evaluate(&res, &hash_buf).unwrap();
            let right = BooleanArray::from(vec![true, true, false]);
            assert_eq!(res, Some(right));

            let res = agg.finalize().unwrap();
            let right = BooleanArray::from(vec![true]);
            assert_eq!(res, right);
        }
    }
}
