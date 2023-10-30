use std::cmp::Ordering;
use std::mem;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::sync::Mutex;

use bincode::serialize;
use crossbeam_skiplist::SkipSet;
use rocksdb::Transaction;
use rocksdb::TransactionDB;
use serde::Deserialize;
use serde::Serialize;

use crate::error::Result;
use crate::options::ReadOptions;
use crate::options::WriteOptions;
use crate::ColValue;
use crate::KeyValue;
use crate::RowValue;
use crate::Value;

pub struct Options {
    pub max_levels: usize,
    pub l0_compaction_threshold: usize,
    pub l0_stop_writes_threshold: usize,
    pub l1_max_bytes: u64,
}

#[derive(Debug)]
pub struct Insert {
    key: Vec<KeyValue>,
    values: Vec<ColValue>,
}

#[derive(Debug)]
pub struct Delete {
    key: Vec<KeyValue>,
}

#[derive(Debug)]
pub enum Op {
    Insert(Insert),
    Delete(Delete),
}

#[derive(Debug)]
struct MemOp {
    op: Op,
    op_id: u64,
}

impl PartialOrd for MemOp {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let left_k = match &self.op {
            Op::Insert(v) => &v.key,
            Op::Delete(v) => &v.key,
        };

        let right_k = match &other.op {
            Op::Insert(v) => &v.key,
            Op::Delete(v) => &v.key,
        };

        match left_k.partial_cmp(right_k) {
            None => unreachable!("keys must be comparable"),
            Some(ord) => match ord {
                Ordering::Equal => self.op_id.partial_cmp(&other.op_id),
                _ => Some(ord),
            },
        }
    }
}

impl PartialEq for MemOp {
    fn eq(&self, other: &Self) -> bool {
        let left_k = match &self.op {
            Op::Insert(v) => &v.key,
            Op::Delete(v) => &v.key,
        };

        let right_k = match &other.op {
            Op::Insert(v) => &v.key,
            Op::Delete(v) => &v.key,
        };

        if left_k == right_k {
            self.op_id == other.op_id
        } else {
            false
        }
    }
}

impl Ord for MemOp {
    fn cmp(&self, other: &Self) -> Ordering {
        self.cmp(other)
    }
}

impl Eq for MemOp {}

#[derive(Clone)]
pub struct OptiDB {
    inner: Arc<OptiDBImpl>,
}

struct OptiDBImpl {
    version: u64,
    op_id: u64,
    pub memtable: Mutex<SkipSet<MemOp>>,
}

impl OptiDBImpl {
    pub fn open() -> Self {
        OptiDBImpl {
            version: 0,
            op_id: 0,
            memtable: Mutex::new(Default::default()),
        }
    }
    pub fn insert(&mut self, key: Vec<KeyValue>, values: Vec<ColValue>) -> Result<()> {
        let op = Op::Insert(Insert { key, values });
        let mem_op = MemOp {
            op,
            op_id: self.op_id,
        };
        let mem = self.memtable.get_mut().unwrap();
        mem.insert(mem_op);
        self.op_id += 1;
        Ok(())
    }
    fn get(&self, col: &str, opts: ReadOptions, key: Vec<Value>) -> Result<Vec<(String, Value)>> {
        unimplemented!()
    }
    fn delete(&self, opts: WriteOptions, key: Vec<Value>) -> Result<()> {
        unimplemented!()
    }
    fn close(&mut self) -> Result<()> {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use crate::db::Insert;
    use crate::db::MemOp;
    use crate::db::Op;
    use crate::db::OptiDBImpl;
    use crate::ColValue;
    use crate::KeyValue;
    use crate::Value;

    #[test]
    fn it_works() {
        let a = MemOp {
            op: Op::Insert(Insert {
                key: vec![
                    KeyValue::String("a".to_string()),
                    KeyValue::String("b".to_string()),
                ],
                values: vec![ColValue {
                    col: "b".to_string(),
                    val: Value::Int8(1),
                }],
            }),
            op_id: 0,
        };

        let b = MemOp {
            op: Op::Insert(Insert {
                key: vec![
                    KeyValue::String("a".to_string()),
                    KeyValue::String("b".to_string()),
                ],
                values: vec![ColValue {
                    col: "b".to_string(),
                    val: Value::Int8(1),
                }],
            }),
            op_id: 1,
        };

        println!("{}", a <= b);
        // let mut db = OptiDBImpl::open();
        // db.insert(
        // vec![
        // KeyValue::String("a".to_string()),
        // KeyValue::String("b".to_string()),
        // ],
        // vec![ColValue {
        // col: "b".to_string(),
        // val: Value::Int8(1),
        // }],
        // )
        // .unwrap();
        //
        // db.insert(
        // vec![
        // KeyValue::String("a".to_string()),
        // KeyValue::String("b".to_string()),
        // ],
        // vec![ColValue {
        // col: "b".to_string(),
        // val: Value::Int8(1),
        // }],
        // )
        // .unwrap();
        // let a = db.memtable.get_mut().unwrap();
        // for i in a.iter() {
        // println!("{:?}", i);
        // }
        // println!("{:?}", db.memtable.lock().unwrap());
    }
}
