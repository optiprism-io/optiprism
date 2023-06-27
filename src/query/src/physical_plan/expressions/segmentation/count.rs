use std::sync::Arc;
use std::sync::Mutex;

use arrow::array::Array;
use arrow::array::ArrayBuilder;
use arrow::array::ArrayRef;
use arrow::array::Int64Builder;
use arrow::array::PrimitiveArray;
use arrow::array::TimestampMillisecondArray;
use arrow::record_batch::RecordBatch;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;

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
    ts_col: Column,
    time_range: TimeRange,
}

impl Count {
    pub fn new(ts_col: Column, time_range: TimeRange) -> Self {
        let inner = CountInner {
            last_hash: 0,
            out: Int64Builder::with_capacity(10_000),
            count: 0,
        };
        Self {
            inner: Arc::new(Mutex::new(inner)),
            ts_col,
            time_range,
        }
    }
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
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::hash_utils::create_hashes;

    use crate::physical_plan::expressions::segmentation::count::Count;
    use crate::physical_plan::expressions::segmentation::time_range::TimeRange;
    use crate::physical_plan::expressions::segmentation::SegmentationExpr;

    #[test]
    fn it_works() {
        let schema = Schema::new(vec![
            Field::new("col1", DataType::Int64, false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
        ]);
        let col: ArrayRef = Arc::new(Int64Array::from(vec![1, 1, 1, 2, 2, 2, 3, 3, 3]));
        let ts: ArrayRef = Arc::new(TimestampMillisecondArray::from(vec![
            1, 2, 3, 1, 2, 3, 1, 2, 3,
        ]));
        let batch =
            RecordBatch::try_new(Arc::new(schema.clone()), vec![col.clone(), ts.clone()]).unwrap();

        let mut random_state = ahash::RandomState::with_seeds(0, 0, 0, 0);
        let mut hash_buf = vec![];
        hash_buf.resize(col.len(), 0);
        create_hashes(&vec![col], &mut random_state, &mut hash_buf).unwrap();
        let mut count = Count::new(
            Column::new_with_schema("ts", &schema).unwrap(),
            TimeRange::None,
        );
        let res = count.evaluate(&batch, &hash_buf).unwrap();
        let right = Arc::new(Int64Array::from(vec![3, 3])) as ArrayRef;
        assert_eq!(res, Some(right));

        let col: ArrayRef = Arc::new(Int64Array::from(vec![3, 3, 3, 4]));
        let ts: ArrayRef = Arc::new(TimestampMillisecondArray::from(vec![1, 2, 3, 1]));
        hash_buf.clear();
        hash_buf.resize(col.len(), 0);
        create_hashes(&vec![col.clone()], &mut random_state, &mut hash_buf).unwrap();
        let batch = RecordBatch::try_new(Arc::new(schema), vec![col.clone(), ts.clone()]).unwrap();
        let res = count.evaluate(&batch, &hash_buf).unwrap();

        let right = Arc::new(Int64Array::from(vec![6])) as ArrayRef;
        assert_eq!(res, Some(right));
        let res = count.finalize().unwrap();
        let right = Arc::new(Int64Array::from(vec![1])) as ArrayRef;
        assert_eq!(&*res, &*right);
    }

    #[test]
    fn time_range() {
        let schema = Schema::new(vec![
            Field::new("col1", DataType::Int64, false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
        ]);
        let col: ArrayRef = Arc::new(Int64Array::from(vec![1, 1, 1, 2, 2, 2, 3]));
        let ts: ArrayRef = Arc::new(TimestampMillisecondArray::from(vec![1, 2, 3, 1, 2, 3, 1]));
        let batch =
            RecordBatch::try_new(Arc::new(schema.clone()), vec![col.clone(), ts.clone()]).unwrap();

        let mut random_state = ahash::RandomState::with_seeds(0, 0, 0, 0);
        let mut hash_buf = vec![];
        hash_buf.resize(col.len(), 0);
        create_hashes(&vec![col], &mut random_state, &mut hash_buf).unwrap();
        let mut count = Count::new(
            Column::new_with_schema("ts", &schema).unwrap(),
            TimeRange::From(2),
        );
        let res = count.evaluate(&batch, &hash_buf).unwrap();
        let right = Arc::new(Int64Array::from(vec![2, 2])) as ArrayRef;
        assert_eq!(res, Some(right));
    }
}