use datafusion_common::ScalarValue;
use std::cmp::Ordering;
use super::error::Result;
use std::sync::atomic::{AtomicUsize, Ordering as MemoryOrdering};
use crate::user_storage::storage::{Op, OpsBucket, OrderedOp};
use std::sync::Arc;

pub struct Memory {
    ops: skiplist::OrderedSkipList<OrderedOp>,
    len: AtomicUsize,
}

impl Memory {
    fn new() -> Self {
        Memory {
            ops: skiplist::OrderedSkipList::new(),
            len: AtomicUsize::new(0),
        }
    }

    pub fn new_empty(&self) -> Self {
        Memory {
            ops: skiplist::OrderedSkipList::new(),
            len: AtomicUsize::new(0),
        }
    }

    pub fn insert_op(&mut self, key: u64, op: Op) -> Result<()> {
        let ordered_op = OrderedOp::new(self.len.fetch_add(1, MemoryOrdering::SeqCst), key, op);
        self.ops.insert(ordered_op);
        Ok(())
    }

    pub fn ops(&self) -> &skiplist::OrderedSkipList<OrderedOp> {
        &self.ops
    }
    pub fn len(&self) -> usize {
        self.ops.len()
    }
}