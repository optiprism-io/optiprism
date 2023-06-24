use std::sync::Arc;
use std::sync::Mutex;

use arrow::array::Array;
use arrow::array::ArrayRef;
use arrow::array::BooleanArray;
use arrow::compute::and;
use arrow::compute::or;
use arrow::record_batch::RecordBatch;

use crate::physical_plan::expressions::segmentation::SegmentationExpr;
#[derive(Debug)]
struct AndInner {
    left: Arc<dyn SegmentationExpr>,
    right: Arc<dyn SegmentationExpr>,
}
#[derive(Debug)]
pub struct And {
    inner: Arc<Mutex<AndInner>>,
}

impl And {
    pub fn new(left: Arc<dyn SegmentationExpr>, right: Arc<dyn SegmentationExpr>) -> Self {
        let inner = Arc::new(Mutex::new(AndInner { left, right }));
        Self { inner }
    }
}

impl SegmentationExpr for And {
    fn evaluate(
        &self,
        record_batch: &RecordBatch,
        hashes: &[u64],
    ) -> crate::Result<Option<ArrayRef>> {
        let mut inner = self.inner.lock().unwrap();
        let left = inner.left.evaluate(record_batch, hashes)?;
        let right = inner.right.evaluate(record_batch, hashes)?;
        match (left, right) {
            (Some(left), Some(right)) => {
                let left = left
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .clone();
                let right = right
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .clone();
                let res = and(&left, &right)?;
                Ok(Some(Arc::new(res) as ArrayRef))
            }
            _ => unreachable!(),
        }
    }

    fn finalize(&self) -> crate::Result<ArrayRef> {
        let mut inner = self.inner.lock().unwrap();
        let left = inner
            .left
            .finalize()?
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .clone();
        let right = inner
            .right
            .finalize()?
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .clone();
        let res = and(&left, &right)?;
        Ok(Arc::new(res) as ArrayRef)
    }
}
#[derive(Debug)]
struct OrInner {
    left: Arc<dyn SegmentationExpr>,
    right: Arc<dyn SegmentationExpr>,
}
#[derive(Debug)]
pub struct Or {
    inner: Arc<Mutex<OrInner>>,
}

impl Or {
    pub fn new(left: Arc<dyn SegmentationExpr>, right: Arc<dyn SegmentationExpr>) -> Self {
        let inner = Arc::new(Mutex::new(OrInner { left, right }));
        Self { inner }
    }
}

impl SegmentationExpr for Or {
    fn evaluate(
        &self,
        record_batch: &RecordBatch,
        hashes: &[u64],
    ) -> crate::Result<Option<ArrayRef>> {
        let mut inner = self.inner.lock().unwrap();
        let left = inner.left.evaluate(record_batch, hashes)?;
        let right = inner.right.evaluate(record_batch, hashes)?;

        match (left, right) {
            (Some(left), Some(right)) => {
                let left = left
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .clone();
                let right = right
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .clone();
                let res = or(&left, &right)?;
                Ok(Some(Arc::new(res) as ArrayRef))
            }
            _ => unreachable!(),
        }
    }

    fn finalize(&self) -> crate::Result<ArrayRef> {
        let mut inner = self.inner.lock().unwrap();
        let left = inner
            .left
            .finalize()?
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .clone();
        let right = inner
            .right
            .finalize()?
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .clone();
        let res = or(&left, &right)?;

        Ok(Arc::new(res) as ArrayRef)
    }
}
