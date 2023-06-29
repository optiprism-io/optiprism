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
use crate::physical_plan::expressions::segmentation::SegmentationExpr;

#[derive(Debug)]
struct CountInner {
    last_hash: u64,
    out: Int64Builder,
    count: i64,
}

#[derive(Debug)]
pub struct Count {
    inner: Arc<Mutex<CountInner>>,
    filter: PhysicalExprRef,
    ts_col: Column,
    time_range: TimeRange,
}

impl Count {
    pub fn new(filter: PhysicalExprRef, ts_col: Column, time_range: TimeRange) -> Self {
        let inner = CountInner {
            last_hash: 0,
            out: Int64Builder::with_capacity(10_000),
            count: 0,
        };
        Self {
            inner: Arc::new(Mutex::new(inner)),
            filter,
            ts_col,
            time_range,
        }
    }
}

fn check_filter(filter: &BooleanArray, idx: usize) -> bool {
    if filter.is_null(idx) {
        return false;
    }
    filter.value(idx)
}

impl SegmentationExpr for Count {
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
                inner.last_hash = *hash;
                let res = inner.count;
                inner.out.append_value(res);
                inner.count = 0;
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
        let res = inner.count;
        inner.out.append_value(res);
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

    use crate::physical_plan::expressions::segmentation::count::Count;
    use crate::physical_plan::expressions::segmentation::time_range::TimeRange;
    use crate::physical_plan::expressions::segmentation::SegmentationExpr;

    #[test]
    fn it_works() {
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
        let left = Arc::new(Column::new_with_schema("event",&res.schema()).unwrap());
        let right = Arc::new(Literal::new(ScalarValue::Utf8(Some("e1".to_string()))));
        let f = BinaryExpr::new(left, Operator::Eq, right);
        let mut count = Count::new(
            Arc::new(f) as PhysicalExprRef,
            Column::new_with_schema("ts", &res.schema()).unwrap(),
            TimeRange::None,
        );
        let res = count.evaluate(&res, &hash_buf).unwrap();
        let right = Arc::new(Int64Array::from(vec![3, 2])) as ArrayRef;
        assert_eq!(res, Some(right));
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
        let left = Arc::new(Column::new_with_schema("event",&res.schema()).unwrap());
        let right = Arc::new(Literal::new(ScalarValue::Utf8(Some("e1".to_string()))));
        let f = BinaryExpr::new(left, Operator::Eq, right);
        let mut count = Count::new(
            Arc::new(f) as PhysicalExprRef,
            Column::new_with_schema("ts", &res.schema()).unwrap(),
            TimeRange::From(2),
        );
        let res = count.evaluate(&res, &hash_buf).unwrap();
        let right = Arc::new(Int64Array::from(vec![2, 2])) as ArrayRef;
        assert_eq!(res, Some(right));
    }
}
