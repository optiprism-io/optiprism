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
use chrono::Duration;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::PhysicalExprRef;

use crate::error::Result;
use crate::physical_plan::abs_row_id;
use crate::physical_plan::expressions::partitioned::boolean_op::ComparisonOp;
use crate::physical_plan::expressions::partitioned::boolean_op::Operator;
use crate::physical_plan::expressions::partitioned::check_filter;
use crate::physical_plan::expressions::partitioned::time_range::TimeRange;
use crate::physical_plan::partitioned_aggregate::PartitionedAggregateExpr;
use crate::physical_plan::Spans;

#[derive(Debug)]
pub struct Count<Op> {
    filter: PhysicalExprRef,
    ts_col: Column,
    time_range: TimeRange,
    op: PhantomData<Op>,
    right: i64,
    time_window: i64,
    is_window: bool,
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
        Self {
            filter,
            ts_col,
            time_range,
            op: Default::default(),
            right,
            time_window: time_window
                .map(|t| t)
                .unwrap_or(Duration::days(365).num_milliseconds()),
            is_window: time_window.is_some(),
            name,
        }
    }
}

impl<Op> Count<Op>
where Op: ComparisonOp<i64>
{
    fn window(
        &self,
        batches: &[RecordBatch],
        spans: Vec<usize>,
        skip: usize,
    ) -> Result<Vec<ArrayRef>> {
        let mut spans = Spans::new_from_batches(spans, batches);
        spans.skip(skip);

        let num_rows = spans.spans.iter().sum::<usize>();
        let mut out = BooleanBuilder::with_capacity(num_rows);
        let ts = batches
            .iter()
            .map(|b| {
                self.ts_col.evaluate(b).and_then(|r| {
                    Ok(r.into_array(b.num_rows())
                        .as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .unwrap()
                        .clone())
                })
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;

        let to_filter = batches
            .iter()
            .map(|b| {
                self.filter.evaluate(b).and_then(|r| {
                    Ok(r.into_array(b.num_rows())
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .unwrap()
                        .clone())
                })
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;

        let mut in_bounds = false;
        let mut window_end = 0;
        let cur_window = 0;
        'next_span: while spans.next_span() {
            let mut count = 0;
            while let Some((batch_id, row_id)) = spans.next_row() {
                let cur_ts = ts[batch_id].value(row_id);
                if !self.time_range.check_bounds(cur_ts) {
                    if in_bounds {
                        break;
                    }
                    continue;
                } else {
                    if !in_bounds {
                        in_bounds = true;
                        window_end = cur_ts + self.time_window - 1;
                    }
                }
                if !check_filter(&to_filter[batch_id], row_id) {
                    continue;
                }
                count += 1;

                println!("cur_ts: {}, window_end: {}", cur_ts, window_end);
                if cur_ts >= window_end {
                    let res = match Op::op() {
                        Operator::Lt => count < self.right,
                        Operator::LtEq => count <= self.right,
                        Operator::Eq => count == self.right,
                        Operator::NotEq => count != self.right,
                        Operator::Gt => count > self.right,
                        Operator::GtEq => count >= self.right,
                    };
                    if !res {
                        println!("? {}", count);
                        out.append_value(false);
                        continue 'next_span;
                    }
                    count = 0;
                    window_end += self.time_window;
                }
            }
            out.append_value(count == 0);
        }

        Ok(vec![Arc::new(out.finish())])
    }

    pub fn non_window(
        &self,
        batches: &[RecordBatch],
        spans: Vec<usize>,
        skip: usize,
    ) -> Result<Vec<ArrayRef>> {
        let mut spans = Spans::new_from_batches(spans, batches);
        spans.skip(skip);

        let num_rows = spans.spans.iter().sum::<usize>();
        let mut out = BooleanBuilder::with_capacity(num_rows);
        let ts = batches
            .iter()
            .map(|b| {
                self.ts_col.evaluate(b).and_then(|r| {
                    Ok(r.into_array(b.num_rows())
                        .as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .unwrap()
                        .clone())
                })
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;

        let to_filter = batches
            .iter()
            .map(|b| {
                self.filter.evaluate(b).and_then(|r| {
                    Ok(r.into_array(b.num_rows())
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .unwrap()
                        .clone())
                })
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;

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

        Ok(vec![Arc::new(out.finish())])
    }
}

impl<Op> PartitionedAggregateExpr for Count<Op>
where Op: ComparisonOp<i64>
{
    fn evaluate(
        &self,
        batches: &[RecordBatch],
        spans: Vec<usize>,
        skip: usize,
    ) -> Result<Vec<ArrayRef>> {
        if !self.is_window {
            self.non_window(batches, spans, skip)
        } else {
            self.window(batches, spans, skip)
        }
    }

    fn fields(&self) -> Vec<Field> {
        vec![Field::new(
            format!("{}_count", self.name),
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
    use crate::physical_plan::expressions::partitioned::cond_count::Count;
    use crate::physical_plan::expressions::partitioned::cond_count::PartitionedAggregateExpr;
    use crate::physical_plan::expressions::partitioned::cond_count::Spans;
    use crate::physical_plan::expressions::partitioned::time_range::TimeRange;

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
        assert!(!span.next_span());
        println!("1 {:?}", span.next_row());
        println!("2 {:?}", span.next_row());
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

            let e: Arc<dyn PartitionedAggregateExpr> = Arc::new(count);
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

            let e: Arc<dyn PartitionedAggregateExpr> = Arc::new(count);
            println!("{:?}", res);
            // assert_eq!(res, Some(right));
            // assert_eq!(res, right);
        }
    }
}
