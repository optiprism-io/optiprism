use arrow::record_batch::RecordBatch;

use crate::error::Result;
use crate::physical_plan::segmentation::SegmentationExpr;

pub struct And {
    left: Box<dyn SegmentationExpr>,
    right: Box<dyn SegmentationExpr>,
}

impl And {
    pub fn new(left: Box<dyn SegmentationExpr>, right: Box<dyn SegmentationExpr>) -> Self {
        Self { left, right }
    }
}

impl SegmentationExpr for And {
    fn evaluate(
        &mut self,
        record_batches: &[RecordBatch],
        spans: Vec<usize>,
        skip: usize,
    ) -> Result<Vec<bool>> {
        let left = self.left.evaluate(record_batches, spans.clone(), skip)?;
        let right = self.right.evaluate(record_batches, spans.clone(), skip)?;
        let mut result = Vec::with_capacity(left.len());
        for i in 0..left.len() {
            result.push(left[i] && right[i]);
        }
        Ok(result)
    }
}
pub struct Or {
    left: Box<dyn SegmentationExpr>,
    right: Box<dyn SegmentationExpr>,
}

impl Or {
    pub fn new(left: Box<dyn SegmentationExpr>, right: Box<dyn SegmentationExpr>) -> Self {
        Self { left, right }
    }
}

impl SegmentationExpr for Or {
    fn evaluate(
        &mut self,
        record_batches: &[RecordBatch],
        spans: Vec<usize>,
        skip: usize,
    ) -> Result<Vec<bool>> {
        let left = self.left.evaluate(record_batches, spans.clone(), skip)?;
        let right = self.right.evaluate(record_batches, spans.clone(), skip)?;
        let mut result = Vec::with_capacity(left.len());
        for i in 0..left.len() {
            result.push(left[i] || right[i]);
        }
        Ok(result)
    }
}
