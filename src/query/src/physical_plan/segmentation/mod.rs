use std::fmt::{Debug, Display};
use arrow::array::ArrayRef;
use arrow::record_batch::RecordBatch;
use datafusion_expr::{ColumnarValue, Operator};
use crate::error::Result;

// pub mod binary_op;
pub mod count;
mod boolean_op;
// pub mod sequence;
// pub mod sum;
pub mod aggregate;
// pub mod sequence_new;
// mod test_value;
// mod boolean_op;

pub trait Expr: Send + Sync + Display + Debug {
    fn evaluate(&mut self, spans: &[usize], batch: &RecordBatch, is_last: bool) -> Result<Option<Vec<i64>>>;
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
        self.span_idx = -1;
        self.row_idx = 0;
        self.batch_len = batch_len;
        self.spans = spans;
        self.result.clear();
    }

    pub fn next_span(&mut self) -> bool {
        if self.span_idx >= 0 {
            println!("[next partition] span idx: {} spans len: {}", self.span_idx, self.spans.len());
        }
        if self.span_idx == self.spans.len() as i64 - 1 {
            println!("[next partition] no next span");

            return false;
        }
        self.span_idx += 1;

        true
    }

    fn cur_span(&self) -> usize {
        self.spans[self.span_idx as usize]
    }
    pub fn next_row(&mut self) -> Option<RowResult> {
        println!("[next row] idx {}, batch len {}", self.row_idx, self.batch_len as i64);
        if self.row_idx == self.batch_len as i64 {
            return None;
        }

        let res = if self.cur_span() == self.row_idx as usize {
            println!("[next row] next partition: cur span {}, row idx {}", self.cur_span(), self.row_idx);
            Some(RowResult::NextPartition(self.row_idx as usize))
        } else {
            Some(RowResult::NextRow(self.row_idx as usize))
        };

        self.row_idx += 1;

        res
    }

    pub fn push_result(&mut self) {
        println!("[push result]: row idx {}", self.row_idx as i64 - 2);
        self.result.push(self.row_idx as i64 - 2);
    }

    pub fn push_final_result(&mut self) {
        println!("[push result]: row idx {}", self.row_idx as i64 - 1);
        self.result.push(self.row_idx as i64 - 1);
    }


    pub fn check_last_span(&self) -> bool {
        println!("{:?}", self.result.last().cloned());
        let res = self.result.last().cloned() != Some(self.row_idx - 1);
        if res {
            println!("[check last span] last idx: {}", self.row_idx - 1)
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