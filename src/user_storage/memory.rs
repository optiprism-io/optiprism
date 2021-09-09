use datafusion::scalar::ScalarValue;
use arrow::datatypes::{SchemaRef, Schema};
use std::cmp::Ordering;
use super::error::Result;
use std::sync::atomic::{AtomicUsize, Ordering as MemoryOrdering};

#[derive(Clone)]
pub enum Op {
    PutProperties(Vec<Option<ScalarValue>>),
    IncrementProperty {
        prop_id: usize,
        delta: ScalarValue,
    },
    DecrementProperty {
        col_id: usize,
        delta: ScalarValue,
    },
    DeleteUser,
}

struct Row {
    row_id: usize,
    user_id: u64,
    op: Op,
}

pub struct Memory {
    rows: skiplist::OrderedSkipList<Row>,
    schema: Schema,
    row_id: AtomicUsize,
}

impl Memory {
    fn new(schema: SchemaRef) -> Self {
        Memory {
            rows: skiplist::OrderedSkipList::new(),
            schema: schema.clone(),
            row_id: AtomicUsize::new(0),
        }
    }

    pub fn new_empty(&self) -> Self {
        Memory {
            rows: skiplist::OrderedSkipList::new(),
            schema: self.schema.clone(),
            row_id: AtomicUsize::new(0),
        }
    }

    pub fn put_properties(&mut self, user_id: u64, props: Vec<Option<ScalarValue>>) -> Result<()> {
        self.insert_op(user_id, Op::PutProperties(props));
        Ok(())
    }

    pub fn insert_op(&mut self, user_id: u64, op: Op) -> Result<()> {
        let row = Row {
            row_id: self.row_id.fetch_add(1, MemoryOrdering::SeqCst),
            user_id,
            op,
        };

        self.rows.insert(row);
        Ok(())
    }
}

impl PartialOrd for Row {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.user_id.partial_cmp(&other.user_id).unwrap().then(self.row_id.partial_cmp(&other.row_id).unwrap()))
    }
}

impl PartialEq for Row {
    fn eq(&self, other: &Self) -> bool {
        self.user_id == other.user_id && self.row_id == other.row_id
    }
}

impl Ord for Row {
    fn cmp(&self, other: &Self) -> Ordering {
        self.user_id.partial_cmp(&other.user_id).unwrap().then(self.row_id.partial_cmp(&other.row_id).unwrap())
    }
}

impl Eq for Row {}