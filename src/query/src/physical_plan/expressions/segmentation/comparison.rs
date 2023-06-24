use std::sync::Arc;

use arrow::array::Array;
use arrow::array::ArrayRef;
use arrow::array::BooleanArray;
use arrow::compute::and;
use arrow::compute::or;
use arrow::record_batch::RecordBatch;

use crate::physical_plan::expressions::segmentation::SegmentationExpr;

#[derive(Debug)]
pub struct And {
    left: Arc<dyn SegmentationExpr>,
    right: Arc<dyn SegmentationExpr>,
}

impl And {
    pub fn new(left: Arc<dyn SegmentationExpr>, right: Arc<dyn SegmentationExpr>) -> Self {
        Self { left, right }
    }
}

impl SegmentationExpr for And {
    fn evaluate(&mut self, record_batch: &RecordBatch, hashes: &[u64]) -> crate::Result<ArrayRef> {
        let left = self
            .left
            .evaluate(record_batch, hashes)?
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        let right = self
            .right
            .evaluate(record_batch, hashes)?
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        let res = and(&left, &right)?;
        Ok(Arc::new(res) as ArrayRef)
    }

    fn finalize(&mut self) -> crate::Result<ArrayRef> {
        let left = self
            .left
            .finalize()?
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        let right = self
            .right
            .finalize()?
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        let res = and(&left, &right)?;
        Ok(Arc::new(res) as ArrayRef)
    }
}

#[derive(Debug)]
pub struct Or {
    left: Arc<dyn SegmentationExpr>,
    right: Arc<dyn SegmentationExpr>,
}

impl Or {
    pub fn new(left: Arc<dyn SegmentationExpr>, right: Arc<dyn SegmentationExpr>) -> Self {
        Self { left, right }
    }
}

impl SegmentationExpr for Or {
    fn evaluate(&mut self, record_batch: &RecordBatch, hashes: &[u64]) -> crate::Result<ArrayRef> {
        let left = self
            .left
            .evaluate(record_batch, hashes)?
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        let right = self
            .right
            .evaluate(record_batch, hashes)?
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        let res = or(&left, &right)?;

        Ok(Arc::new(res) as ArrayRef)
    }

    fn finalize(&mut self) -> crate::Result<ArrayRef> {
        let left = self
            .left
            .finalize()?
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        let right = self
            .right
            .finalize()?
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        let res = or(&left, &right)?;

        Ok(Arc::new(res) as ArrayRef)
    }
}
