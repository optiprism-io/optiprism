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

pub trait PartitionedAggregateExpr {
    fn evaluate(
        &self,
        batches: &[RecordBatch],
        spans: &[usize],
        skip: usize,
    ) -> Result<Vec<ArrayRef>>;
    fn column_names(&self) -> &[&str];
}

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

impl<Op> PartitionedAggregateExpr for Count<Op>
where Op: ComparisonOp<i64>
{
    fn evaluate(
        &self,
        batches: &[RecordBatch],
        spans: &[usize],
        skip: usize,
    ) -> Result<Vec<ArrayRef>> {
        let num_rows = batches.iter().map(|b| b.num_rows()).sum::<usize>();

        let offset = if skip > 0 { skip } else { 0 };
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

        let mut count = 0;
        let mut span_id = 0;
        for idx in offset..num_rows {
            let (row_id, batch_id) = abs_row_id(idx, batches);
            if check_filter(&to_filter[batch_id], row_id) == false {
                // println!("skip1");
                continue;
            }
            if !self.time_range.check_bounds(ts[batch_id].value(row_id)) {
                // println!("skip2");
                continue;
            }
            count += 1;
            if idx - offset == spans[span_id] {
                span_id += 1;
                let res = match Op::op() {
                    Operator::Lt => count < self.right,
                    Operator::LtEq => count <= self.right,
                    Operator::Eq => count == self.right,
                    Operator::NotEq => count != self.right,
                    Operator::Gt => count > self.right,
                    Operator::GtEq => count >= self.right,
                };
                if !res {
                    // println!("a1 false {}", last_hash);
                    out.append_value(false);
                    continue;
                }
                // println!("a2 true {}", last_hash);
                out.append_value(true);
            }
        }

        Ok(vec![Arc::new(out.finish())])
    }

    fn column_names(&self) -> &[&str] {
        &["count"]
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
    use crate::physical_plan::expressions::segmentation::time_range::TimeRange;

    #[test]
    fn test_predicate() {
        let data = vec![
            r#"
| user_id(i64) | ts(ts) | event(utf8) |
|--------------|--------|-------------|
| 0            | 1      | e1          |
| 0            | 2      | e2          |
| 0            | 3      | e3          |
| 0            | 4      | e1          |
| 0            | 5      | e1          |
| 0            | 6      | e2          |
| 0            | 7      | e3          |
"#,
            r#"
| 1            | 8      | e1          |
| 1            | 9      | e3          |
| 1            | 10      | e1          |
| 1            | 11      | e2          |
"#,
            r#"
| 2            | 12      | e2          |
| 2            | 13      | e1          |
| 2            | 14      | e2          |
"#,
        ];

        let res = data
            .iter()
            .map(|d| parse_markdown_table_v1(*d).unwrap())
            .collect::<Vec<_>>();
        let spans = vec![7, 4, 3];

        {
            let left = Arc::new(Column::new_with_schema("event", &res[0].schema()).unwrap());
            let right = Arc::new(Literal::new(ScalarValue::Utf8(Some("e1".to_string()))));
            let f = BinaryExpr::new(left, Operator::Eq, right);
            let mut count = Count::<Gt>::new(
                Arc::new(f) as PhysicalExprRef,
                Column::new_with_schema("ts", &res[0].schema()).unwrap(),
                2,
                TimeRange::None,
                None,
            );

            let res = count.evaluate(&res, &spans, 0).unwrap();
            let right = BooleanArray::from(vec![true, false]);
            println!("{:?}", right);
            // assert_eq!(res, Some(right));
            // assert_eq!(res, right);
        }
    }
}
