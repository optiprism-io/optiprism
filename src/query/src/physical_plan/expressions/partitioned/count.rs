use std::collections::BinaryHeap;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::Mutex;

use arrow::array::Array;
use arrow::array::ArrayBuilder;
use arrow::array::ArrayRef;
use arrow::array::BooleanArray;
use arrow::array::BooleanBuilder;
use arrow::array::Int64Array;
use arrow::array::Int64Builder;
use arrow::array::PrimitiveArray;
use arrow::array::TimestampMillisecondArray;
use arrow::compute::filter;
use arrow::compute::filter_record_batch;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use arrow::datatypes::SchemaRef;
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
    outer_fn: Vec<AggregateFunction>,
}

#[derive(Debug)]
pub struct Count {
    filter: Option<PhysicalExprRef>,
    inner: Mutex<Inner>,
    name: String,
}

impl Count {
    pub fn new(
        filter: Option<PhysicalExprRef>,
        outer_fn: AggregateFunction,
        segments: usize,
        name: String,
    ) -> Self {
        let inner = Inner {
            last_filter: None,
            outer_fn: (0..segments)
                .into_iter()
                .map(|_| outer_fn.clone())
                .collect(),
        };
        Self {
            filter,
            inner: Mutex::new(inner),

            name,
        }
    }
}

impl PartitionedAggregateExpr for Count {
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
        if self.filter.is_none() {
            let v = spans.spans.iter().map(|v| *v as i128).collect::<Vec<_>>();

            for (span_id, count) in v.into_iter().enumerate() {
                for (idx, seg) in segments[span_id].iter().enumerate() {
                    if *seg {
                        inner.outer_fn[idx].accumulate(count);
                    }
                }
            }
        }

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

        while spans.next_span() {
            let mut count = 0;
            while let Some((batch_id, row_id)) = spans.next_row() {
                if let Some(to_filter) = &to_filter {
                    if !check_filter(&to_filter[batch_id], row_id) {
                        continue;
                    }
                }

                count += 1;
            }

            for (seg_idx, seg) in segments[spans.span_id as usize].iter().enumerate() {
                if *seg {
                    inner.outer_fn[seg_idx].accumulate(count);
                }
            }
            count = 0;
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Array;
    use arrow::array::ArrayRef;
    use arrow::array::BooleanArray;
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

    use crate::physical_plan::expressions::partitioned::count::Count;
    use crate::physical_plan::expressions::partitioned::AggregateFunction;
    use crate::physical_plan::expressions::partitioned::PartitionedAggregateExpr;

    #[test]
    fn test_predicate() {
        let data = r#"
| user_id(i64) | ts(ts) | event(utf8) |
|--------------|--------|-------------|
| 0            | 1      | 1           |
| 0            | 2      | 1           |
|              |        |             |
| 1            | 8      | 1           |
|              |        |             |
| 2            | 1      | 2           |
| 2            | 2      | 1           |
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

        {
            let left = Arc::new(Column::new_with_schema("event", &res[0].schema()).unwrap());
            let right = Arc::new(Literal::new(ScalarValue::Utf8(Some("1".to_string()))));
            let f = BinaryExpr::new(left, Operator::Eq, right);
            let mut count = Count::new(
                Some(Arc::new(f) as PhysicalExprRef),
                AggregateFunction::new_avg(),
                2,
                "a".to_string(),
            );

            let spans = vec![2, 1, 2];
            count
                .evaluate(&res, spans, 0, vec![
                    vec![true, true],
                    vec![true, true],
                    vec![true, true],
                ])
                .unwrap();
            let inner = count.inner.lock().unwrap();
            for v in inner.outer_fn.iter() {
                println!("{:?}", v.result());
            }
        }
    }
}
