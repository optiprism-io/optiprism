use datafusion::scalar::ScalarValue;
use std::cmp::Ordering;
use super::error::Result;
use std::sync::atomic::{AtomicUsize, Ordering as MemoryOrdering};
use crate::user_storage::storage::{Op, Row, OpsBucket, OrderedOp};
use std::sync::Arc;

pub struct Memory {
    ops: skiplist::OrderedSkipList<OrderedOp>,
    cols: Vec<Vec<usize>>,
    next_row_id: AtomicUsize,
}

impl Memory {
    fn new() -> Self {
        Memory {
            ops: skiplist::OrderedSkipList::new(),
            next_row_id: AtomicUsize::new(0),
        }
    }

    pub fn new_empty(&self) -> Self {
        Memory {
            ops: skiplist::OrderedSkipList::new(),
            next_row_id: AtomicUsize::new(0),
        }
    }

    pub fn insert_op(&mut self, user_id: u64, op: Op) -> Result<()> {
        let row = Row::new(self.next_row_id.fetch_add(1, MemoryOrdering::SeqCst), user_id, op);
        self.ops.insert(row);
        Ok(())
    }

    pub fn ops(&self) -> &skiplist::OrderedSkipList<OrderedOp> {
        &self.ops
    }
    pub fn len(&self) -> usize {
        self.rows.len()
    }
}