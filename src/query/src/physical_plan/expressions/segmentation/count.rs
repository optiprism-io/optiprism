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
use arrow::buffer::ScalarBuffer;
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
use crate::physical_plan::expressions::check_filter;
use crate::physical_plan::expressions::segmentation::boolean_op::ComparisonOp;
use crate::physical_plan::expressions::segmentation::boolean_op::Operator;
use crate::physical_plan::expressions::segmentation::time_range::TimeRange;
use crate::physical_plan::expressions::segmentation::SegmentExpr;

#[derive(Debug)]
struct Inner {
    count: i64,
    last_partition: i64,
    res: Int64Builder,
    first: bool,
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
    out_batch_size: usize,
}

impl<Op> Count<Op> {
    pub fn new(
        filter: PhysicalExprRef,
        ts_col: Column,
        right: i64,
        time_range: TimeRange,
        time_window: Option<i64>,
        out_batch_size: usize,
    ) -> Self {
        let inner = Inner {
            count: 0,
            last_partition: 0,
            res: Int64Builder::with_capacity(1000),
            first: true,
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
            out_batch_size,
        }
    }
}

impl<'a, Op> SegmentExpr for Count<Op>
where Op: ComparisonOp<i64>
{
    fn evaluate(
        &self,
        batch: &RecordBatch,
        partitions: &ScalarBuffer<i64>,
    ) -> Result<Option<Int64Array>> {
        let ts = self
            .ts_col
            .evaluate(batch)?
            .into_array(batch.num_rows())
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap()
            .clone();
        let filter = self
            .filter
            .evaluate(batch)?
            .into_array(batch.num_rows())
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .clone();

        let mut inner = self.inner.lock().unwrap();
        for (row_id, partition) in partitions.into_iter().enumerate() {
            if inner.first {
                inner.first = false;
                inner.last_partition = *partition;
            }
            if !self.time_range.check_bounds(ts.value(row_id)) {
                continue;
            }

            if !check_filter(&filter, row_id) {
                continue;
            }

            if inner.last_partition != *partition {
                let res = match Op::op() {
                    Operator::Lt => inner.count < self.right,
                    Operator::LtEq => inner.count <= self.right,
                    Operator::Eq => inner.count == self.right,
                    Operator::NotEq => inner.count != self.right,
                    Operator::Gt => inner.count > self.right,
                    Operator::GtEq => inner.count >= self.right,
                };

                if !res {
                    inner.res.append_null();
                } else {
                    let v = inner.last_partition;
                    inner.res.append_value(v);
                }
                inner.last_partition = *partition;

                inner.count = 0;
            }
            inner.count += 1;
        }
        if inner.res.len() > self.out_batch_size {
            Ok(Some(inner.res.finish()))
        } else {
            Ok(None)
        }
    }

    fn finalize(&self) -> Result<Int64Array> {
        let mut inner = self.inner.lock().unwrap();
        let res = match Op::op() {
            Operator::Lt => inner.count < self.right,
            Operator::LtEq => inner.count <= self.right,
            Operator::Eq => inner.count == self.right,
            Operator::NotEq => inner.count != self.right,
            Operator::Gt => inner.count > self.right,
            Operator::GtEq => inner.count >= self.right,
        };

        if !res {
            inner.res.append_null();
        } else {
            let v = inner.last_partition;
            inner.res.append_value(v);
        }

        Ok(inner.res.finish())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Array;
    use arrow::array::Int64Array;
    use arrow2::array::Int32Array;
    use datafusion::physical_expr::expressions::BinaryExpr;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::expressions::Literal;
    use datafusion::physical_expr::PhysicalExprRef;
    use datafusion_common::ScalarValue;
    use datafusion_expr::Operator;
    use store::test_util::parse_markdown_tables;

    use crate::physical_plan::expressions::segmentation::boolean_op;
    use crate::physical_plan::expressions::segmentation::count::Count;
    use crate::physical_plan::expressions::segmentation::time_range::TimeRange;
    use crate::physical_plan::expressions::segmentation::SegmentExpr;

    #[test]
    fn it_works() {
        let data = r#"
| user_id(i64) | ts(ts) | event(utf8) |
|--------------|--------|-------------|
| 0            | 1      | 1          |
| 0            | 2      | 1          |
|              |        |             |
| 1            | 8      | 1          |
|              |        |             |
| 1            | 1      | 1          |
| 1            | 2      | 1          |
|              |        |             |
| 2            | 8      | 1          |
| 2            | 8      | 1          |
| 2            | 8      | 1          |
| 2            | 8      | 1          |
| 3            | 8      | 1          |
"#;
        let res = parse_markdown_tables(data).unwrap();

        {
            let left = Arc::new(Column::new_with_schema("event", &res[0].schema()).unwrap());
            let right = Arc::new(Literal::new(ScalarValue::Utf8(Some("1".to_string()))));
            let f = BinaryExpr::new(left, Operator::Eq, right);
            let mut count = Count::<boolean_op::Gt>::new(
                Arc::new(f) as PhysicalExprRef,
                Column::new_with_schema("ts", &res[0].schema()).unwrap(),
                2,
                TimeRange::None,
                None,
                1,
            );

            for b in res {
                let p = b.columns()[0]
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values();

                let res = count.evaluate(&b, p).unwrap();
                println!("{:?}", res);
            }
            let res = count.finalize().unwrap();
            println!("{:?}", res);
        }
    }
}