use std::sync::Arc;
use std::sync::Mutex;

use arrow::array::Int64Array;
use arrow::array::Int64Builder;
use arrow::buffer::ScalarBuffer;
use arrow::record_batch::RecordBatch;

use crate::error::Result;
use crate::physical_plan::expressions::segmentation::SegmentExpr;

#[derive(Debug)]
struct AndInner {
    left: Arc<dyn SegmentExpr>,
    right: Arc<dyn SegmentExpr>,
}

#[derive(Debug)]
pub struct And {
    inner: Arc<Mutex<AndInner>>,
}

impl And {
    pub fn new(left: Arc<dyn SegmentExpr>, right: Arc<dyn SegmentExpr>) -> Self {
        let inner = Arc::new(Mutex::new(AndInner { left, right }));
        Self { inner }
    }

    pub fn and(left: Int64Array, right: Int64Array) -> Int64Array {
        let mut out = Int64Builder::with_capacity(left.len());
        left.iter().zip(right.iter()).for_each(|(l, r)| {
            if let Some(l) = l
                && r.is_some()
            {
                out.append_value(l);
            } else {
                out.append_null();
            }
        });

        out.finish()
    }
}

impl SegmentExpr for And {
    fn evaluate(
        &self,
        batch: &RecordBatch,
        partitions: &ScalarBuffer<i64>,
    ) -> Result<()> {
        let inner = self.inner.lock().unwrap();
        inner.left.evaluate(batch, partitions)?;
        inner.right.evaluate(batch, partitions)?;

        Ok(())
    }

    fn finalize(&self) -> Result<Int64Array> {
        let inner = self.inner.lock().unwrap();
        Ok(Self::and(inner.left.finalize()?, inner.right.finalize()?))
    }
}

#[derive(Debug)]
pub struct Or {
    inner: Arc<Mutex<AndInner>>,
}

impl Or {
    pub fn new(left: Arc<dyn SegmentExpr>, right: Arc<dyn SegmentExpr>) -> Self {
        let inner = Arc::new(Mutex::new(AndInner { left, right }));
        Self { inner }
    }

    pub fn or(left: Int64Array, right: Int64Array) -> Int64Array {
        let mut out = Int64Builder::with_capacity(left.len());

        left.iter().zip(right.iter()).for_each(|(l, r)| {
            if l.is_some() || r.is_some() {
                out.append_value(l.or(r).unwrap());
            } else {
                out.append_null();
            }
        });

        out.finish()
    }
}

impl SegmentExpr for Or {
    fn evaluate(
        &self,
        batch: &RecordBatch,
        partitions: &ScalarBuffer<i64>,
    ) -> Result<()> {
        let inner = self.inner.lock().unwrap();
        inner.left.evaluate(batch, partitions)?;
        inner.right.evaluate(batch, partitions)?;
        Ok(())
    }

    fn finalize(&self) -> Result<Int64Array> {
        let inner = self.inner.lock().unwrap();
        Ok(Self::or(inner.left.finalize()?, inner.right.finalize()?))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int64Array;
    use arrow::buffer::ScalarBuffer;
    use arrow::datatypes::DataType;
    use arrow::datatypes::Field;
    use arrow::datatypes::Schema;
    use arrow::record_batch::RecordBatch;

    use crate::error::Result;
    use crate::physical_plan::expressions::segmentation::comparison::And;
    use crate::physical_plan::expressions::segmentation::comparison::Or;
    use crate::physical_plan::expressions::segmentation::SegmentExpr;

    #[derive(Debug)]
    struct Test {
        a: Option<Int64Array>,
        f: Int64Array,
    }

    impl SegmentExpr for Test {
        fn evaluate(
            &self,
            _batch: &RecordBatch,
            _partitions: &ScalarBuffer<i64>,
        ) -> Result<()> {
            Ok(())
        }

        fn finalize(&self) -> Result<Int64Array> {
            Ok(self.f.clone())
        }
    }

    #[test]
    fn and() {
        let a = Test {
            a: Some(Int64Array::from(vec![None, Some(2), Some(3)])),
            f: Int64Array::from(vec![Some(4), None, Some(6)]),
        };

        let b = Test {
            a: Some(Int64Array::from(vec![Some(4), None, Some(6)])),
            f: Int64Array::from(vec![4, 5, 6]),
        };

        let and = And::new(Arc::new(a), Arc::new(b));

        let schema = Schema::new(vec![Field::new("sdf", DataType::Boolean, true)]);
        let rb = &RecordBatch::new_empty(Arc::new(schema));
        let res = and
            .evaluate(rb, &ScalarBuffer::from(vec![1, 2, 3]))
            .unwrap();

        dbg!(&res);
    }

    #[test]
    fn or() {
        let a = Test {
            a: Some(Int64Array::from(vec![None, Some(2), None])),
            f: Int64Array::from(vec![Some(4), None, None]),
        };

        let b = Test {
            a: Some(Int64Array::from(vec![Some(4), None, None])),
            f: Int64Array::from(vec![4, 5, 6]),
        };

        let and = Or::new(Arc::new(a), Arc::new(b));

        let schema = Schema::new(vec![Field::new("sdf", DataType::Boolean, true)]);
        let rb = &RecordBatch::new_empty(Arc::new(schema));
        let _res = and
            .evaluate(rb, &ScalarBuffer::from(vec![1, 2, 3]))
            .unwrap();
    }
}
