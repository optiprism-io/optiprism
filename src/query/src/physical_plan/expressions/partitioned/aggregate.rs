use std::collections::BinaryHeap;
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
use arrow::array::PrimitiveArray;
use arrow::array::PrimitiveBuilder;
use arrow::array::TimestampMillisecondArray;
use arrow::array::UInt16Array;
use arrow::array::UInt32Array;
use arrow::array::UInt64Array;
use arrow::array::UInt8Array;
use arrow::compute::filter;
use arrow::compute::filter_record_batch;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use arrow::datatypes::SchemaRef;
use arrow::ipc::DecimalBuilder;
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::print_batches;
use chrono::Duration;
use common::DECIMAL_PRECISION;
use common::DECIMAL_SCALE;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::PhysicalExprRef;
use datafusion_common::ScalarValue;
use datafusion_expr::ColumnarValue;
use num_traits::Bounded;
use num_traits::Num;
use num_traits::NumCast;
use num_traits::Zero;

use crate::error::Result;
use crate::physical_plan::abs_row_id;
use crate::physical_plan::batch_id;
use crate::physical_plan::expressions::check_filter;
use crate::physical_plan::expressions::partitioned::AggregateFunction;
use crate::physical_plan::expressions::partitioned::PartitionedAggregateExpr;
use crate::physical_plan::Spans;

#[derive(Debug)]
struct Inner {
    last_filter: Option<(usize, BooleanArray)>,
    last_predicate: Option<(usize, ArrayRef)>,
    agg: AggregateFunction,
    outer_fn: Vec<AggregateFunction>,
}

#[derive(Debug)]
pub struct Aggregate<T> {
    inner: Mutex<Inner>,
    filter: Option<PhysicalExprRef>,
    predicate: Column,
    name: String,
    t: PhantomData<T>,
}

impl<T> Aggregate<T> {
    pub fn new(
        filter: Option<PhysicalExprRef>,
        predicate: Column,
        agg: AggregateFunction,
        outer_fn: AggregateFunction,
        segments: usize,
        name: String,
    ) -> Self {
        let inner = Inner {
            last_filter: None,
            last_predicate: None,
            agg,
            outer_fn: (0..segments)
                .into_iter()
                .map(|_| outer_fn.clone())
                .collect(),
        };
        Self {
            filter,
            predicate,
            inner: Mutex::new(inner),
            name,
            t: Default::default(),
        }
    }
}

macro_rules! agg {
    ($ty:ty,$arr:ident) => {
        impl PartitionedAggregateExpr for Aggregate<$ty> {
            fn evaluate(
                &self,
                batches: &[RecordBatch],
                spans: Vec<usize>,
                skip: usize,
                segments: Vec<Vec<bool>>,
            ) -> Result<()> {
                let mut spans = Spans::new_from_batches(spans, batches);
                spans.skip(skip);

                let mut inner = self.inner.lock().unwrap();

                let to_filter = {
                    if let Some(filter) = self.filter.clone() {
                        let to_filter = batches
                            .iter()
                            .enumerate()
                            .map(|(idx, b)| {
                                if let Some((id, a)) = &inner.last_filter {
                                    if *id == batch_id(b) {
                                        return Ok(a.to_owned());
                                    }
                                }
                                return filter.evaluate(b).and_then(|r| {
                                    Ok(r.into_array(b.num_rows())
                                        .as_any()
                                        .downcast_ref::<BooleanArray>()
                                        .unwrap()
                                        .clone())
                                });
                            })
                            .collect::<std::result::Result<Vec<_>, _>>()?;

                        inner.last_filter = Some((
                            batch_id(batches.last().unwrap()),
                            to_filter.last().unwrap().clone(),
                        ));

                        Some(to_filter)
                    } else {
                        None
                    }
                };

                let arr = {
                    batches
                        .iter()
                        .enumerate()
                        .map(|(idx, b)| {
                            if let Some((id, a)) = &inner.last_predicate {
                                if *id == batch_id(b) {
                                    return Ok(a
                                        .to_owned()
                                        .as_any()
                                        .downcast_ref::<$arr>()
                                        .unwrap()
                                        .clone());
                                }
                            }
                            self.predicate.evaluate(b).and_then(|r| {
                                Ok(r.into_array(b.num_rows())
                                    .as_any()
                                    .downcast_ref::<$arr>()
                                    .unwrap()
                                    .clone())
                            })
                        })
                        .collect::<std::result::Result<Vec<_>, _>>()?
                };

                inner.last_predicate = Some((
                    batch_id(batches.last().unwrap()),
                    Arc::new(arr.last().unwrap().clone()),
                ));

                while spans.next_span() {
                    while let Some((batch_id, row_id)) = spans.next_row() {
                        if let Some(to_filter) = &to_filter {
                            if !check_filter(&to_filter[batch_id], row_id) {
                                continue;
                            }
                        }

                        inner.agg.accumulate(arr[batch_id].value(row_id) as i128);
                    }

                    for (seg_idx, seg) in segments[spans.span_id as usize].iter().enumerate() {
                        if *seg {
                            let res = inner.agg.result();
                            inner.outer_fn[seg_idx].accumulate(res);
                        }
                    }
                    inner.agg.reset();
                }

                Ok(())
            }

            fn data_types(&self) -> Vec<DataType> {
                vec![DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE)]
            }

            fn finalize(&self) -> Vec<Vec<ColumnarValue>> {
                let inner = self.inner.lock().unwrap();
                let res = inner
                    .outer_fn
                    .iter()
                    .map(|v| {
                        vec![ColumnarValue::Scalar(ScalarValue::Decimal128(
                            Some(v.result()),
                            DECIMAL_PRECISION,
                            DECIMAL_SCALE,
                        ))]
                    })
                    .collect::<Vec<_>>();
                res
            }
        }
    };
}

agg!(i64, Int64Array);
agg!(i128, Decimal128Array);
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Array;
    use arrow::array::ArrayRef;
    use arrow::array::BooleanArray;
    use arrow::array::Decimal128Array;
    use arrow::array::Int64Array;
    use arrow::array::TimestampMillisecondArray;
    use arrow::datatypes::DataType;
    use arrow::datatypes::DataType::Duration;
    use arrow::datatypes::Field;
    use arrow::datatypes::Schema;
    use arrow::datatypes::SchemaRef;
    use arrow::datatypes::TimeUnit;
    use arrow::record_batch::RecordBatch;
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
    use store::test_util::parse_markdown_tables;

    use crate::physical_plan::expressions::partitioned::aggregate::Aggregate;
    use crate::physical_plan::expressions::partitioned::AggregateFunction;
    use crate::physical_plan::expressions::partitioned::PartitionedAggregateExpr;

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
            let mut agg = Aggregate::<i64>::new(
                None,
                Column::new_with_schema("v", &schema).unwrap(),
                AggregateFunction::new_sum(),
                AggregateFunction::new_avg(),
                2,
                "1".to_string(),
            );
            let spans = vec![7, 4, 3];
            agg.evaluate(&res, spans, 0, vec![
                vec![true, true],
                vec![true, true],
                vec![true, true],
            ])
            .unwrap();
            println!("{:?}", agg.finalize());
            // let right = Decimal128Array::from(vec![2, 4, 2]);
            // assert_eq!(res, vec![Arc::new(right) as ArrayRef]);
        }
    }

    #[test]
    fn test_decimal() {
        let data = r#"
| user_id(i64) | ts(ts) | event(utf8) | v(decimal) |
|--------------|--------|-------------|--------|
| 0            | 1      | e1          | 1.1      |
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
            let mut agg = Aggregate::<i128>::new(
                None,
                Column::new_with_schema("v", &schema).unwrap(),
                AggregateFunction::new_sum(),
                AggregateFunction::new_avg(),
                2,
                "1".to_string(),
            );
            let spans = vec![7, 4, 3];
            agg.evaluate(&res, spans, 0, vec![
                vec![true, true],
                vec![true, true],
                vec![true, true],
            ])
            .unwrap();
            println!("{:?}", agg.finalize());
            // let right = Decimal128Array::from(vec![2, 4, 2]);
            // assert_eq!(res, vec![Arc::new(right) as ArrayRef]);
        }
    }
}
