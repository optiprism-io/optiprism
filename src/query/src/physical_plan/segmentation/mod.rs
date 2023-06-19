use std::fmt::Debug;
use std::fmt::Display;

use arrow::array::ArrayRef;
use arrow::record_batch::RecordBatch;
use datafusion_expr::ColumnarValue;
use datafusion_expr::Operator;
use tracing::debug;

use crate::error::Result;

// pub mod binary_op;
mod boolean_op;
pub mod count;
// pub mod sequence;
// pub mod sum;
pub mod aggregate;
pub mod sum;
pub mod time_window;
// mod sequence;
// mod test_value;
// mod boolean_op;

pub trait EvaluationResult {
    fn spans(&self) -> Option<Vec<i64>>;
    fn as_any(&self) -> &dyn std::any::Any;
}

pub trait Expr: Send + Sync + Display + Debug {
    fn evaluate(
        &mut self,
        spans: &[usize],
        batch: &RecordBatch,
        is_last: bool,
    ) -> Result<Option<Vec<i64>>>;
}

#[derive(Debug)]
pub struct Spans {
    span_idx: i64,
    row_idx: i64,
    spans: Vec<usize>,
    batch_len: usize,
    result: Vec<i64>,
}

pub enum RowResult {
    NextPartition(usize),
    NextRow(usize),
}

impl Spans {
    pub fn new(result_size: usize) -> Self {
        Self {
            span_idx: -1,
            row_idx: 0,
            spans: Vec::new(),
            batch_len: 0,
            result: Vec::with_capacity(result_size),
        }
    }

    pub fn reset(&mut self, spans: Vec<usize>, batch_len: usize) {
        debug!("\n");
        self.span_idx = -1;
        self.row_idx = 0;
        self.batch_len = batch_len;
        self.spans = spans;
        self.result.clear();
    }

    pub fn next_span(&mut self) -> bool {
        if self.span_idx >= 0 {
            debug!(
                "[next partition] span idx: {} spans len: {}",
                self.span_idx,
                self.spans.len()
            );
        }
        if self.span_idx == self.spans.len() as i64 - 1 {
            debug!("[next partition] no next span");

            return false;
        }
        self.span_idx += 1;

        true
    }

    fn cur_span(&self) -> usize {
        self.spans[self.span_idx as usize]
    }
    pub fn next_row(&mut self) -> Option<RowResult> {
        debug!(
            "[next row] idx {}, batch len {}",
            self.row_idx, self.batch_len as i64
        );
        if self.row_idx == self.batch_len as i64 {
            return None;
        }

        let res = if self.cur_span() == self.row_idx as usize {
            debug!(
                "[next row] next partition: cur span {}, row idx {}",
                self.cur_span(),
                self.row_idx
            );
            Some(RowResult::NextPartition(self.row_idx as usize))
        } else {
            Some(RowResult::NextRow(self.row_idx as usize))
        };

        self.row_idx += 1;

        res
    }

    pub fn push_result(&mut self) {
        debug!("[push result]: row idx {}", self.row_idx as i64 - 2);
        self.result.push(self.row_idx as i64 - 2);
    }

    pub fn push_final_result(&mut self) {
        debug!("[push result]: row idx {}", self.row_idx as i64 - 1);
        self.result.push(self.row_idx as i64 - 1);
    }

    pub fn check_last_span(&self) -> bool {
        debug!("{:?}", self.result.last().cloned());
        let res = self.result.last().cloned() != Some(self.row_idx - 1);
        if res {
            debug!("[check last span] last idx: {}", self.row_idx - 1)
        }

        res
    }

    pub fn result(&mut self) -> Option<Vec<i64>> {
        if self.result.is_empty() {
            None
        } else {
            Some(self.result.drain(..).collect())
        }
    }
}
