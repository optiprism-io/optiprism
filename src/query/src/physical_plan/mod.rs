use std::sync::Arc;
use arrow::record_batch::RecordBatch;
use datafusion::physical_expr::hash_utils::create_hashes;
use datafusion::physical_expr::PhysicalExpr;
use datafusion_common::Result as DFResult;

mod dictionary_decode;
pub mod expressions;
pub mod merge;
mod pivot;
pub mod planner;
mod unpivot;
mod segmentation;
mod funnel;
// pub mod merge;
// pub mod planner;

struct PartitionState {
    random_state: ahash::RandomState,
    hash_buffer: Vec<u64>,
    buf: Vec<RecordBatch>,
    last_value: Option<u64>,
    spans: Vec<usize>,
    partition_key: Vec<Arc<dyn PhysicalExpr>>,
}

impl PartitionState {
    pub fn new(partition_key: Vec<Arc<dyn PhysicalExpr>>) -> Self {
        Self {
            random_state: ahash::RandomState::with_seeds(0, 0, 0, 0),
            hash_buffer: vec![],
            buf: Vec::with_capacity(10),
            last_value: None,
            spans: vec![0],
            partition_key,
        }
    }

    pub fn push(&mut self, batch: RecordBatch) -> DFResult<Option<(Vec<RecordBatch>, Vec<usize>)>> {
        self.buf.push(batch.clone());
        let arrays = self.partition_key
            .iter()
            .map(|expr| {
                Ok(expr.evaluate(&batch)?.into_array(batch.num_rows()))
            })
            .collect::<DFResult<Vec<_>>>()?;
        let num_rows = batch.num_rows();

        self.hash_buffer.clear();
        self.hash_buffer.resize(num_rows, 0);
        create_hashes(&arrays, &mut self.random_state, &mut self.hash_buffer)?;

        let mut take = false;

        for (idx, v) in self.hash_buffer.iter().enumerate() {
            if self.last_value.is_none() {
                self.last_value = Some(*v);
            }

            if self.last_value != Some(*v) {
                self.spans.push(0);
                take = true;
            }
            let i = self.spans.len() - 1;
            self.spans[i] += 1;
            self.last_value = Some(*v);
        };


        if self.buf.len() > 1 && take {
            let mut take_batches = self.buf.drain(..self.buf.len() - 1).collect::<Vec<_>>();
            take_batches.push(self.buf.last().unwrap().to_owned());
            let take_spans = self.spans.drain(..self.spans.len() - 1).collect::<Vec<usize>>();

            return Ok(Some((take_batches, take_spans)));
        }
        Ok(None)
    }

    pub fn finalize(&mut self) -> DFResult<Option<(Vec<RecordBatch>, Vec<usize>)>> {
        if self.spans.len() > 0 {
            let batches = self.buf.drain(0..).collect();
            let spans = self.spans.drain(0..).collect();

            return Ok(Some((batches, spans)));
        }

        Ok(None)
    }
}