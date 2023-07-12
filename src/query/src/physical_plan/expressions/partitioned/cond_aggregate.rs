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
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::PhysicalExprRef;
use num_traits::Bounded;
use num_traits::Num;
use num_traits::NumCast;
use num_traits::Zero;

use crate::error::Result;
use crate::physical_plan::abs_row_id;
use crate::physical_plan::batch_id;
use crate::physical_plan::expressions::partitioned::boolean_op::ComparisonOp;
use crate::physical_plan::expressions::partitioned::boolean_op::Operator;
use crate::physical_plan::expressions::partitioned::check_filter;
use crate::physical_plan::expressions::partitioned::time_range::TimeRange;
use crate::physical_plan::expressions::partitioned::AggregateFunction;
use crate::physical_plan::partitioned_aggregate::PartitionedAggregateExpr;
use crate::physical_plan::Spans;

#[derive(Debug)]
struct Inner<OT>
where OT: Copy + Num + Bounded + NumCast + PartialOrd + Clone
{
    last_ts: Option<(usize, TimestampMillisecondArray)>,
    last_filter: Option<(usize, BooleanArray)>,
    last_predicate: Option<(usize, ArrayRef)>,
    agg: AggregateFunction<OT>,
}

#[derive(Debug)]
pub struct Aggregate<T, OT, Op>
where OT: Copy + Num + Bounded + NumCast + PartialOrd + Clone
{
    inner: Mutex<Inner<OT>>,
    filter: PhysicalExprRef,
    predicate: Column,
    ts_col: Column,
    time_range: TimeRange,
    op: PhantomData<Op>,
    right: OT,
    typ: PhantomData<T>,
    time_window: i64,
    name: String,
}

impl<T, OT, Op> Aggregate<T, OT, Op>
where OT: Copy + Num + Bounded + NumCast + PartialOrd + Clone
{
    pub fn new(
        filter: PhysicalExprRef,
        predicate: Column,
        agg: AggregateFunction<OT>,
        ts_col: Column,
        right: OT,
        time_range: TimeRange,
        time_window: Option<i64>,
        name: String,
    ) -> Self {
        let inner = Inner {
            last_ts: None,
            last_filter: None,
            last_predicate: None,
            agg,
        };
        Self {
            filter,
            predicate,
            ts_col,
            inner: Mutex::new(inner),
            time_range,
            op: Default::default(),
            right,
            typ: Default::default(),
            time_window: time_window
                .map(|t| t)
                .unwrap_or(Duration::days(365).num_milliseconds()),
            name,
        }
    }
}

impl<Op> PartitionedAggregateExpr for Aggregate<i64, i128, Op>
where Op: ComparisonOp<i128>
{
    fn evaluate(
        &self,
        batches: &[RecordBatch],
        spans: Vec<usize>,
        skip: usize,
    ) -> Result<Vec<ArrayRef>> {
        let mut spans = Spans::new_from_batches(spans, batches);
        spans.skip(skip);

        let num_rows = spans.spans.iter().sum::<usize>();
        let mut out = BooleanBuilder::with_capacity(num_rows);

        let mut inner = self.inner.lock().unwrap();
        let ts = {
            batches
                .iter()
                .enumerate()
                .map(|(idx, b)| {
                    if let Some((id, ts)) = &inner.last_ts {
                        if *id == batch_id(b) {
                            return Ok(ts.to_owned());
                        }
                    }
                    return self.ts_col.evaluate(b).and_then(|r| {
                        Ok(r.into_array(b.num_rows())
                            .as_any()
                            .downcast_ref::<TimestampMillisecondArray>()
                            .unwrap()
                            .clone())
                    });
                })
                .collect::<std::result::Result<Vec<_>, _>>()?
        };

        inner.last_ts = Some((
            batch_id(batches.last().unwrap()),
            ts.last().unwrap().clone(),
        ));

        let to_filter = {
            batches
                .iter()
                .enumerate()
                .map(|(idx, b)| {
                    if let Some((id, a)) = &inner.last_filter {
                        if *id == batch_id(b) {
                            return Ok(a.to_owned());
                        }
                    }
                    return self.filter.evaluate(b).and_then(|r| {
                        Ok(r.into_array(b.num_rows())
                            .as_any()
                            .downcast_ref::<BooleanArray>()
                            .unwrap()
                            .clone())
                    });
                })
                .collect::<std::result::Result<Vec<_>, _>>()?
        };

        inner.last_filter = Some((
            batch_id(batches.last().unwrap()),
            to_filter.last().unwrap().clone(),
        ));

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
                                .downcast_ref::<Int64Array>()
                                .unwrap()
                                .clone());
                        }
                    }
                    return self.predicate.evaluate(b).and_then(|r| {
                        Ok(r.into_array(b.num_rows())
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap()
                            .clone())
                    });
                })
                .collect::<std::result::Result<Vec<_>, _>>()?
        };

        inner.last_predicate = Some((
            batch_id(batches.last().unwrap()),
            Arc::new(arr.last().unwrap().clone()),
        ));

        while spans.next_span() {
            while let Some((batch_id, row_id)) = spans.next_row() {
                if !self.time_range.check_bounds(ts[batch_id].value(row_id)) {
                    continue;
                }

                if !check_filter(&to_filter[batch_id], row_id) {
                    continue;
                }

                inner.agg.accumulate(arr[batch_id].value(row_id) as i128);
            }

            let res = Op::perform(inner.agg.result(), self.right);
            if !res {
                out.append_value(false);
            } else {
                out.append_value(true);
            }

            inner.agg.reset();
        }

        Ok(vec![Arc::new(out.finish())])
    }

    fn fields(&self) -> Vec<Field> {
        vec![Field::new(
            format!("{}_agg", self.name),
            DataType::Boolean,
            false,
        )]
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(self.fields()))
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

    use crate::physical_plan::expressions::partitioned::boolean_op;
    use crate::physical_plan::expressions::partitioned::boolean_op::Gt;
    use crate::physical_plan::expressions::partitioned::cond_aggregate::Aggregate;
    use crate::physical_plan::expressions::partitioned::time_range::TimeRange;
    use crate::physical_plan::expressions::partitioned::AggregateFunction;
    use crate::physical_plan::partitioned_aggregate::PartitionedAggregateExpr;

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
            let mut agg = Aggregate::<i64, i128, Gt>::new(
                Arc::new(f) as PhysicalExprRef,
                Column::new_with_schema("v", &schema).unwrap(),
                AggregateFunction::new_sum(),
                Column::new_with_schema("ts", &schema).unwrap(),
                3,
                TimeRange::None,
                None,
                "1".to_string(),
            );
            let spans = vec![7, 4, 3];
            let res = agg.evaluate(&res, spans, 0).unwrap();
            let right = BooleanArray::from(vec![false, true]);
            println!("{:?}", res);
            // assert_eq!(res, Some(right));
        }
    }
}
