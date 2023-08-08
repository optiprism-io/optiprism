use std::path::PathBuf;
use crate::storage::ids::EventId;


#[derive(Debug, Clone)]
pub enum Op {
    PutValues(Vec<Option<ScalarValue>>),
    IncrementValue {
        col_id: usize,
        delta: ScalarValue,
    },
    DecrementValue {
        col_id: usize,
        delta: ScalarValue,
    },
    DeleteKey,
    None,
}

#[derive(Clone)]
pub struct OrderedOp {
    order: usize,
    key: u64,
    pub op: Op,
}

pub trait StorageSnapshot {}

pub trait OptiEngine: Send + Sync {
    type StorageSnapshot;
    //type OperationResult;
    
    fn open(&self, path: PathBuf);
    fn close(&mut self);
    fn shapshot(&self) -> Self::StorageSnapshot;
    fn exec_operation(&mut self, op: Operation) -> Result<(), ()>; //Self::OperationResult;
    //fn put();
    //fn get();
    //fn delete();
}

