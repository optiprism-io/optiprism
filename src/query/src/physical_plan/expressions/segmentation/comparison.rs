use std::sync::Arc;
use std::sync::Mutex;

use arrow::array::Array;
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

    pub fn and(left: Option<Int64Array>, right: Option<Int64Array>) -> Option<Int64Array> {
        match (left, right) {
            (Some(left), Some(right)) => {
                let mut out = Int64Builder::with_capacity(left.len());

                left.iter().zip(right.iter()).for_each(|(l, r)| {
                    if l.is_some() && r.is_some() {
                        out.append_value(l.unwrap());
                    } else {
                        out.append_null();
                    }
                });

                return Some(out.finish());
            }
            (None, None) => return None,
            _ => unreachable!(),
        };
    }
}

impl SegmentExpr for And {
    fn evaluate(
        &self,
        batch: &RecordBatch,
        partitions: &ScalarBuffer<i64>,
    ) -> Result<Option<Int64Array>> {
        let mut inner = self.inner.lock().unwrap();
        let left = inner.left.evaluate(batch, partitions)?;
        let right = inner.right.evaluate(batch, partitions)?;
        Ok(Self::and(left, right))
    }

    fn finalize(&self) -> Result<Int64Array> {
        let mut inner = self.inner.lock().unwrap();
        Ok(Self::and(Some(inner.left.finalize()?), Some(inner.right.finalize()?)).unwrap())
    }
}

struct OrInner {
    left: Arc<dyn SegmentExpr>,
    right: Arc<dyn SegmentExpr>,
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

    pub fn or(left: Option<Int64Array>, right: Option<Int64Array>) -> Option<Int64Array> {
        match (left, right) {
            (Some(left), Some(right)) => {
                let mut out = Int64Builder::with_capacity(left.len());

                left.iter().zip(right.iter()).for_each(|(l, r)| {
                    if l.is_some() || r.is_some() {
                        out.append_value(l.or(r).unwrap());
                    } else {
                        out.append_null();
                    }
                });

                return Some(out.finish());
            }
            (None, None) => return None,
            _ => unreachable!(),
        };
    }
}

impl SegmentExpr for Or {
    fn evaluate(
        &self,
        batch: &RecordBatch,
        partitions: &ScalarBuffer<i64>,
    ) -> Result<Option<Int64Array>> {
        let mut inner = self.inner.lock().unwrap();
        let left = inner.left.evaluate(batch, partitions)?;
        let right = inner.right.evaluate(batch, partitions)?;
        Ok(Self::or(left, right))
    }

    fn finalize(&self) -> Result<Int64Array> {
        let mut inner = self.inner.lock().unwrap();
        Ok(Self::or(Some(inner.left.finalize()?), Some(inner.right.finalize()?)).unwrap())
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

    impl Test {
        fn new(a: Option<Int64Array>, f: Int64Array) -> Self {
            Self { a, f }
        }
    }

    impl SegmentExpr for Test {
        fn evaluate(
            &self,
            batch: &RecordBatch,
            partitions: &ScalarBuffer<i64>,
        ) -> Result<Option<Int64Array>> {
            Ok(self.a.clone())
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

        println!("{:?}", res);
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
        let res = and
            .evaluate(rb, &ScalarBuffer::from(vec![1, 2, 3]))
            .unwrap();

        println!("{:?}", res);
    }
}
