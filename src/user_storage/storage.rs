use crate::user_storage::memory::{Op, Memory};
use arrow::datatypes::Schema;
use datafusion::scalar::ScalarValue;
use super::error::Result;
use core::mem;

pub struct Storage {
    schema: Schema,
    memory: Memory,
    write_buffer_size: usize,
}

impl Storage {
    pub fn put_properties(&mut self, user_id: u64, props: Vec<Option<ScalarValue>>) -> Result<()> {
        self.insert_op(user_id, Op::PutProperties(props))
    }

    fn insert_op(&mut self, user_id: u64, op: Op) -> Result<()> {
        self.memory.insert_op(user_id, op)?;

        if self.memory.len() >= self.write_buffer_size {
            let mut empty = self.memory.new_empty();
            let immutable = mem::replace(&mut self.memory, empty);
        }
        Ok(())
    }
}