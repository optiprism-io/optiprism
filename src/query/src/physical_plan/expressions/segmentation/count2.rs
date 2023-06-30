use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::Mutex;

use arrow::array::{Array, BooleanArray};
use arrow::array::ArrayBuilder;
use arrow::array::ArrayRef;
use arrow::array::Int64Builder;
use arrow::array::PrimitiveArray;
use arrow::array::TimestampMillisecondArray;
use arrow::compute::{filter, filter_record_batch};
use arrow::record_batch::RecordBatch;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{PhysicalExpr, PhysicalExprRef};

use crate::error::Result;
use crate::physical_plan::expressions::segmentation::time_range::TimeRange;
use crate::physical_plan::expressions::segmentation::{check_filter, SegmentationExpr};
use crate::physical_plan::expressions::segmentation::boolean_op::{ComparisonOp, Operator};

#[derive(Debug)]
struct CountInner {
    last_hash: u64,
    out: Int64Builder,
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
}

impl<Op> Count<Op> {
    pub fn new(filter: PhysicalExprRef, ts_col: Column, right: i64, time_range: TimeRange) -> Self {
        let inner = CountInner {
            last_hash: 0,
            out: Int64Builder::with_capacity(10_000),
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
        }
    }
}

impl<Op> SegmentationExpr for Count<Op> where Op: ComparisonOp<i64> {
    fn evaluate(&self, record_batch: &RecordBatch, hashes: &[u64]) -> Result<Option<ArrayRef>> {
        let ts = self
            .ts_col
            .evaluate(record_batch)?
            .into_array(record_batch.num_rows())
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap()
            .clone();

        let to_filter = self.filter.evaluate(record_batch)?.into_array(record_batch.num_rows()).as_any().downcast_ref::<BooleanArray>().unwrap().clone();
        let mut inner = self.inner.lock().unwrap();
        for (idx, hash) in hashes.iter().enumerate() {
            if inner.last_hash == 0 {
                inner.last_hash = *hash;
            }

            if *hash != inner.last_hash {
                let count = inner.count;
                inner.count = 0;

                inner.last_hash = *hash;

                let res = match Op::op() {
                    Operator::Lt => count < self.right,
                    Operator::LtEq => count <= self.right,
                    Operator::Eq => count == self.right,
                    Operator::NotEq => count != self.right,
                    Operator::Gt => count > self.right,
                    Operator::GtEq => count >= self.right,
                };
                if !res {
                    inner.count += 1;
                    continue;
                }

                inner.out.append_value(count);
            }

            if inner.skip {
                continue;
            }

            if check_filter(&to_filter, idx) == false {
                continue;
            }
            if !self.time_range.check_bounds(ts.value(idx)) {
                continue;
            }

            inner.count += 1;
        }

        if inner.out.len() > 0 {
            Ok(Some(Arc::new(inner.out.finish()) as ArrayRef))
        } else {
            Ok(None)
        }
    }

    fn finalize(&self) -> Result<ArrayRef> {
        let mut inner = self.inner.lock().unwrap();
        if inner.skip {
            return Ok(Arc::new(inner.out.finish()) as ArrayRef);
        }

        let count = inner.count;
        let res = match Op::op() {
            Operator::Lt => count < self.right,
            Operator::LtEq => count <= self.right,
            Operator::Eq => count == self.right,
            Operator::NotEq => count != self.right,
            Operator::Gt => count > self.right,
            Operator::GtEq => count >= self.right,
        };

        if !res {
            return Ok(Arc::new(inner.out.finish()) as ArrayRef);
        }
        inner.out.append_value(count);
        Ok(Arc::new(inner.out.finish()) as ArrayRef)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Array;
    use arrow::array::ArrayRef;
    use arrow::array::Int64Array;
    use arrow::array::TimestampMillisecondArray;
    use arrow::datatypes::DataType;
    use arrow::datatypes::Field;
    use arrow::datatypes::Schema;
    use arrow::datatypes::SchemaRef;
    use arrow::datatypes::TimeUnit;
    use arrow::record_batch::RecordBatch;
    use datafusion::physical_expr::hash_utils::create_hashes;
    use datafusion::physical_expr::{expressions, PhysicalExprRef};
    use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
    use datafusion_common::ScalarValue;
    use datafusion_expr::{binary_expr, Expr, lit, Operator};
    use store::test_util::parse_markdown_table_v1;
    use crate::physical_plan::expressions::segmentation::boolean_op::Gt;
    use crate::physical_plan::expressions::segmentation::count2::Count;

    use crate::physical_plan::expressions::segmentation::time_range::TimeRange;
    use crate::physical_plan::expressions::segmentation::SegmentationExpr;

    #[test]
    fn test_predicate() {
        let data = r#"
| user_id(i64) | ts(ts) | event(utf8) |
|--------------|--------|-------------|
| 0            | 1      | e1          |
| 0            | 2      | e2          |
| 0            | 3      | e3          |
| 0            | 4      | e1          |
| 0            | 5      | e1          |
| 0            | 6      | e2          |
| 0            | 7      | e3          |
| 1            | 8      | e1          |
| 1            | 9      | e3          |
| 1            | 10      | e1          |
| 1            | 11      | e2          |
| 2            | 12      | e1          |
| 2            | 13      | e1          |
| 2            | 14      | e1          |
"#;

        let res = parse_markdown_table_v1(data).unwrap();

        let mut random_state = ahash::RandomState::with_seeds(0, 0, 0, 0);
        let mut hash_buf = vec![];
        hash_buf.resize(res.num_rows(), 0);
        create_hashes(&vec![res.columns()[0].clone()], &mut random_state, &mut hash_buf).unwrap();

        {
            let left = Arc::new(Column::new_with_schema("event", &res.schema()).unwrap());
            let right = Arc::new(Literal::new(ScalarValue::Utf8(Some("e1".to_string()))));
            let f = BinaryExpr::new(left, Operator::Eq, right);
            let mut count = Count::<Gt>::new(
                Arc::new(f) as PhysicalExprRef,
                Column::new_with_schema("ts", &res.schema()).unwrap(),
                2,
                TimeRange::None,
            );
            let res = count.evaluate(&res, &hash_buf).unwrap();
            let right = Arc::new(Int64Array::from(vec![3])) as ArrayRef;
            assert_eq!(res, Some(right));

            let res = count.finalize().unwrap();
            let right = Arc::new(Int64Array::from(vec![3])) as ArrayRef;
            assert_eq!(&*res, &*right);
        }
        {
            let left = Arc::new(Column::new_with_schema("event", &res.schema()).unwrap());
            let right = Arc::new(Literal::new(ScalarValue::Utf8(Some("e2".to_string()))));
            let f = BinaryExpr::new(left, Operator::Eq, right);
            let mut count = Count::<Gt>::new(
                Arc::new(f) as PhysicalExprRef,
                Column::new_with_schema("ts", &res.schema()).unwrap(),
                1,
                TimeRange::None,
            );
            let res = count.evaluate(&res, &hash_buf).unwrap();
            let right = Arc::new(Int64Array::from(vec![3])) as ArrayRef;
            assert_eq!(res, Some(right));

            let res = count.finalize().unwrap();
            let right = Arc::new(Int64Array::from(vec![3])) as ArrayRef;
            assert_eq!(&*res, &*right);
        }
    }

    #[test]
    fn time_range() {
        let data = r#"
    | user_id(i64) | ts(ts) | event(utf8) |
    |--------------|--------|-------------|
    | 0            | 1      | e1          |
    | 0            | 2      | e2          |
    | 0            | 3      | e3          |
    | 0            | 4      | e1          |
    | 0            | 5      | e1          |
    | 0            | 6      | e2          |
    | 0            | 7      | e3          |
    | 1            | 5      | e1          |
    | 1            | 6      | e3          |
    | 1            | 7      | e1          |
    | 1            | 8      | e2          |
    | 2            | 9      | e1          |
    "#;

        let res = parse_markdown_table_v1(data).unwrap();

        let mut random_state = ahash::RandomState::with_seeds(0, 0, 0, 0);
        let mut hash_buf = vec![];
        hash_buf.resize(res.num_rows(), 0);
        create_hashes(&vec![res.columns()[0].clone()], &mut random_state, &mut hash_buf).unwrap();
        let left = Arc::new(Column::new_with_schema("event", &res.schema()).unwrap());
        let right = Arc::new(Literal::new(ScalarValue::Utf8(Some("e1".to_string()))));
        let f = BinaryExpr::new(left, Operator::Eq, right);
        let mut count = Count::<Gt>::new(
            Arc::new(f) as PhysicalExprRef,
            Column::new_with_schema("ts", &res.schema()).unwrap(),
            1,
            TimeRange::From(2),
        );
        let res = count.evaluate(&res, &hash_buf).unwrap();
        let right = Arc::new(Int64Array::from(vec![2, 2])) as ArrayRef;
        assert_eq!(res, Some(right));
    }

    #[test]
    fn time_window() {
        let data = r#"
    | user_id(i64) | ts(ts) | event(utf8) |
    |--------------|--------|-------------|
    | 0            | 1      | e1          |
    | 0            | 2      | e1          |
    | 0            | 3      | e1          |
    | 0            | 4      | e1          |
    | 0            | 5      | e1          |
    | 0            | 6      | e1          |
    | 0            | 7      | e1          |
    | 1            | 5      | e1          |
    | 1            | 6      | e1          |
    | 1            | 7      | e1          |
    | 1            | 8      | e1          |
    | 2            | 9      | e1          |
    "#;

        let res = parse_markdown_table_v1(data).unwrap();

        let mut random_state = ahash::RandomState::with_seeds(0, 0, 0, 0);
        let mut hash_buf = vec![];
        hash_buf.resize(res.num_rows(), 0);
        create_hashes(&vec![res.columns()[0].clone()], &mut random_state, &mut hash_buf).unwrap();
        let left = Arc::new(Column::new_with_schema("event", &res.schema()).unwrap());
        let right = Arc::new(Literal::new(ScalarValue::Utf8(Some("e1".to_string()))));
        let f = BinaryExpr::new(left, Operator::Eq, right);
        let mut count = Count::<Gt>::new(
            Arc::new(f) as PhysicalExprRef,
            Column::new_with_schema("ts", &res.schema()).unwrap(),
            1,
            TimeRange::From(2),
        );
        let res = count.evaluate(&res, &hash_buf).unwrap();
        let right = Arc::new(Int64Array::from(vec![2, 2])) as ArrayRef;
        assert_eq!(res, Some(right));
    }
}
