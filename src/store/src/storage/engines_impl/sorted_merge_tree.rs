use std::path::PathBuf;

use skiplist::OrderedSkipList;

use crate::storage::engine::OptiEngine;
use crate::storage::engine::OrderedOp;
use crate::storage::wal::OptiWal;

pub struct Segment {
    index: BloomFilter, //?? self-reference structure
    path: PathBuf, // parquet file path
    data: //??
}

pub struct SortedMergeTree {
    wal: OptiWal,
    memory: OrderedSkipList<OrderedOp>,
    levels: Vec<(usize, Segment)>
}

pub struct SortedMergeTreeSnapshot {}

impl OptiEngine for SortedMergeTree {
    type StorageSnapshot = SortedMergeTreeSnapshot;

    type OperationResult;

    fn open(&self, path: std::path::PathBuf) {
        todo!()
    }

    fn close(&mut self) {
        todo!()
    }

    fn shapshot(&self) -> Self::StorageSnapshot {
        todo!()
    }

    fn exec_operation(&mut self, op: Self::Operation) -> Self::OperationResult {
        todo!()
    }
}
