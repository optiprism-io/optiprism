use std::collections::BinaryHeap;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::Mutex;

use arrow::array::Array;
use arrow::array::ArrayBuilder;
use arrow::array::ArrayRef;
use arrow::array::BooleanArray;
use arrow::array::BooleanBuilder;
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

use crate::error::Result;
use crate::physical_plan::abs_row_id;
use crate::physical_plan::batch_id;
use crate::physical_plan::expressions::check_filter;
use crate::physical_plan::expressions::segmentation::boolean_op::ComparisonOp;
use crate::physical_plan::expressions::segmentation::boolean_op::Operator;
use crate::physical_plan::expressions::segmentation::time_range::TimeRange;
use crate::physical_plan::expressions::segmentation::SegmentedAggregateExpr;
use crate::physical_plan::Spans;

#[derive(Debug)]
struct Inner {
    last_ts: Option<(usize, TimestampMillisecondArray)>,
    last_filter: Option<(usize, BooleanArray)>,
}

#[derive(Debug)]
pub struct Count<Op> {
    filter: PhysicalExprRef,
    ts_col: Column,
    inner: Mutex<Inner>,
    time_range: TimeRange,
    op: PhantomData<Op>,
    right: i64,
    time_window: i64,
    name: String,
}

impl<Op> Count<Op> {
    pub fn new(
        filter: PhysicalExprRef,
        ts_col: Column,
        right: i64,
        time_range: TimeRange,
        time_window: Option<i64>,
        name: String,
    ) -> Self {
        let inner = Inner {
            last_ts: None,
            last_filter: None,
        };
        Self {
            filter,
            ts_col,
            inner: Mutex::new(inner),
            time_range,
            op: Default::default(),
            right,
            time_window: time_window
                .map(|t| t)
                .unwrap_or(Duration::days(365).num_milliseconds()),
            name,
        }
    }
}

impl<Op> SegmentedAggregateExpr for Count<Op>
where Op: ComparisonOp<i64>
{
    fn evaluate(
        &self,
        batches: &[RecordBatch],
        spans: Vec<usize>,
        skip: usize,
    ) -> Result<BooleanArray> {
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
                    if let Some((id, a)) = &inner.last_ts {
                        if *id == batch_id(b) {
                            return Ok(a.to_owned());
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

        while spans.next_span() {
            let mut count = 0;
            while let Some((batch_id, row_id)) = spans.next_row() {
                if !self.time_range.check_bounds(ts[batch_id].value(row_id)) {
                    continue;
                }

                if !check_filter(&to_filter[batch_id], row_id) {
                    continue;
                }

                count += 1;
            }

            let res = match Op::op() {
                Operator::Lt => count < self.right,
                Operator::LtEq => count <= self.right,
                Operator::Eq => count == self.right,
                Operator::NotEq => count != self.right,
                Operator::Gt => count > self.right,
                Operator::GtEq => count >= self.right,
            };
            count = 0;
            if !res {
                out.append_value(false);
            } else {
                out.append_value(true);
            }
        }
        Ok(out.finish())
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

    use crate::physical_plan::expressions::segmentation::boolean_op;
    use crate::physical_plan::expressions::segmentation::count::Count;
    use crate::physical_plan::expressions::segmentation::time_range::TimeRange;
    use crate::physical_plan::expressions::segmentation::SegmentedAggregateExpr;
    use crate::physical_plan::Spans;

    #[test]
    fn test_spans() {
        let mut span = Spans::new(vec![1, 2, 3], vec![1, 2, 6]);

        assert!(span.next_span());
        println!("1 {:?}", span.next_row());
        println!("2 {:?}", span.next_row());
        println!("3 {:?}", span.next_row());
        assert!(span.next_span());
        println!("1 {:?}", span.next_row());
        println!("2 {:?}", span.next_row());
        println!("3 {:?}", span.next_row());
        assert!(span.next_span());
        println!("1 {:?}", span.next_row());
        println!("2 {:?}", span.next_row());
        println!("3 {:?}", span.next_row());

        let mut span = Spans::new(vec![10], vec![13]);
        span.skip(10);
        assert!(span.next_span());
        println!("1 {:?}", span.next_row());
        println!("2 {:?}", span.next_row());
        println!("3 {:?}", span.next_row());
        assert!(span.next_span());
        println!("3 {:?}", span.next_row());
        println!("3 {:?}", span.next_row());
        println!("3 {:?}", span.next_row());
    }

    #[test]
    fn test_predicate() {
        let data = r#"
| user_id(i64) | ts(ts) | event(utf8) |
|--------------|--------|-------------|
| 0            | 1      | 1          |
| 0            | 2      | 1          |
|              |        |             |
| 1            | 8      | 1          |
|              |        |             |
| 0            | 1      | 1          |
| 0            | 2      | 1          |
|              |        |             |
| 1            | 8      | 1          |
| 1            | 8      | 1          |
| 1            | 8      | 1          |
| 1            | 8      | 1          |
| 1            | 8      | 1          |
"#;
        let res = parse_markdown_tables(data).unwrap();

        {
            let left = Arc::new(Column::new_with_schema("event", &res[0].schema()).unwrap());
            let right = Arc::new(Literal::new(ScalarValue::Utf8(Some("1".to_string()))));
            let f = BinaryExpr::new(left, Operator::Eq, right);
            let mut count = Count::<boolean_op::Eq>::new(
                Arc::new(f) as PhysicalExprRef,
                Column::new_with_schema("ts", &res[0].schema()).unwrap(),
                1,
                TimeRange::None,
                None,
                "a".to_string(),
            );

            let spans = vec![2, 1, 2, 1];
            let res = count.evaluate(&res, spans, 0).unwrap();
            let right = BooleanArray::from(vec![true, false]);

            let e: Arc<dyn SegmentedAggregateExpr> = Arc::new(count);
            println!("{:?}", res);
            // assert_eq!(res, Some(right));
            // assert_eq!(res, right);
        }
    }

    #[test]
    fn test_window() {
        let data = r#"
| user_id(i64) | ts(ts) | event(utf8) |
|--------------|--------|-------------|
| 0            | 1      | 1          |
| 0            | 2      | 1          |
| 0            | 3      | 1          |
| 0            | 4      | 1          |
| 0            | 5      | 1          |
| 1            | 6      | 1          |
| 1            | 7      | 1          |
| 1            | 8      | 1          |
| 1            | 9      | 1          |
"#;
        let res = parse_markdown_tables(data).unwrap();

        {
            let left = Arc::new(Column::new_with_schema("event", &res[0].schema()).unwrap());
            let right = Arc::new(Literal::new(ScalarValue::Utf8(Some("1".to_string()))));
            let f = BinaryExpr::new(left, Operator::Eq, right);
            let mut count = Count::<boolean_op::Eq>::new(
                Arc::new(f) as PhysicalExprRef,
                Column::new_with_schema("ts", &res[0].schema()).unwrap(),
                1,
                TimeRange::None,
                Some(1),
                "a".to_string(),
            );

            let spans = vec![5, 4];
            let res = count.evaluate(&res, spans, 0).unwrap();
            let right = BooleanArray::from(vec![true, false]);

            let e: Arc<dyn SegmentedAggregateExpr> = Arc::new(count);
            println!("{:?}", res);
            // assert_eq!(res, Some(right));
            // assert_eq!(res, right);
        }
    }
}
