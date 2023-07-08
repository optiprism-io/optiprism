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
use arrow::record_batch::RecordBatch;
use chrono::Duration;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::PhysicalExprRef;

use crate::error::Result;
use crate::physical_plan::abs_row_id;
use crate::physical_plan::expressions::segmentation::boolean_op::ComparisonOp;
use crate::physical_plan::expressions::segmentation::boolean_op::Operator;
use crate::physical_plan::expressions::segmentation::check_filter;
use crate::physical_plan::expressions::segmentation::time_range::TimeRange;
use crate::physical_plan::partitioned_aggregate::PartitionedAggregateExpr;

#[derive(Debug)]
struct CountInner {
    last_hash: u64,
    last_ts: i64,
    out: BooleanBuilder,
    count: i64,
    skip: bool,
}

#[derive(Debug)]
pub struct Count<Op> {
    inner: Arc<Mutex<CountInner>>,
    filter: PhysicalExprRef,
    ts_col: Column,
    time_range: TimeRange,
    op: PhantomData<Op>,
    right: i64,
    time_window: i64,
}

impl<Op> Count<Op> {
    pub fn new(
        filter: PhysicalExprRef,
        ts_col: Column,
        right: i64,
        time_range: TimeRange,
        time_window: Option<i64>,
    ) -> Self {
        let inner = CountInner {
            last_hash: 0,
            last_ts: 0,
            out: BooleanBuilder::with_capacity(10_000),
            count: 0,
            skip: false,
        };
        Self {
            inner: Arc::new(Mutex::new(inner)),
            filter,
            ts_col,
            time_range,
            op: Default::default(),
            right,
            time_window: time_window
                .map(|t| t)
                .unwrap_or(Duration::days(365).num_milliseconds()),
        }
    }
}

struct Span {
    spans: Vec<usize>,
    span_id: i64,
    row_id: usize,
    offset: usize,
    batches: Vec<usize>,
}

impl Span {
    pub fn new(spans: Vec<usize>, batches: Vec<usize>) -> Self {
        Self {
            spans,
            span_id: -1,
            row_id: 0,
            offset: 0,
            batches,
        }
    }

    // returns next span if exist
    pub fn next_span(&mut self) -> bool {
        if self.span_id >= self.spans.len() as i64 - 1 {
            return false;
        }
        self.span_id += 1;
        self.row_id = 0;
        if self.span_id == 0 {
            return true;
        }
        self.offset += self.spans[self.span_id as usize];

        true
    }
    // return next batch_id and row_id of span if exist
    pub fn next_row(&mut self) -> Option<(usize, usize)> {
        if self.span_id >= self.spans.len() as i64 {
            return None;
        }
        if self.row_id >= self.spans[self.span_id as usize] {
            return None;
        }

        let mut batch_id = 0;
        let mut row_id = self.row_id + self.offset;
        while row_id >= self.batches[batch_id] {
            row_id -= self.batches[batch_id];
            batch_id += 1;
        }

        self.row_id += 1;

        Some((batch_id, row_id))
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
        println!("ev");
        let spans = if skip > 0 {
            vec![vec![skip], spans].concat()
        } else {
            spans
        };

        let mut spans = Span::new(spans, batches.iter().map(|b| b.num_rows()).collect());

        if skip > 0 {
            spans.next_span();
        }

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
                count += 1;

                if !check_filter(&to_filter[batch_id], row_id) {
                    continue;
                }

                if !self.time_range.check_bounds(ts[batch_id].value(row_id)) {
                    continue;
                }
            }

            println!("count: {}", count);
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

    fn fields(&self) -> Vec<Field> {
        vec![Field::new("count", DataType::Int64, false)]
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
    use store::test_util::parse_markdown_tables;

    use crate::physical_plan::expressions::segmentation::boolean_op;
    use crate::physical_plan::expressions::segmentation::boolean_op::Gt;
    use crate::physical_plan::expressions::segmentation::count_span::Count;
    use crate::physical_plan::expressions::segmentation::count_span::PartitionedAggregateExpr;
    use crate::physical_plan::expressions::segmentation::count_span::Span;
    use crate::physical_plan::expressions::segmentation::time_range::TimeRange;

    #[test]
    fn test_spans() {
        let mut span = Span::new(vec![1, 2, 3], vec![1, 2, 6]);

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
}
