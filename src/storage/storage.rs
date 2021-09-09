use crate::storage::memory::Memory;
use super::error::Result;
use datafusion::scalar::ScalarValue;
use crate::storage::log::Log;
use std::sync::mpsc::{channel, Sender, Receiver};
use std::sync::Arc;
use core::mem;
use arrow::datatypes::SchemaRef;


// row is a set of columns
pub type Row = Vec<ScalarValue>;


#[derive(Clone)]
pub enum Order {
    Asc(usize),
    Desc(usize),
}

struct Part {
    id: usize,
    level: usize,
    size_in_bytes: usize,
    size_in_rows: usize,
}

struct Level {
    parts: Vec<Part>,
    max_size_in_bytes: Option<usize>,
    max_size_in_rows: Option<usize>,
}

pub struct Table {
    order: Vec<Order>,
    schema: SchemaRef,
    log: Log,
    memory: Memory,
    write_buffer_size: usize,
    flush_queue_sender: Sender<FlushTask>,
    flash_queue_receiver: Receiver<FlushTask>,
    levels: Vec<Level>,
    last_part_id: usize,
    index: Vec<Part>,
}

struct FlushTask {
    mem: Memory,
    part_id: usize,
    schema: SchemaRef,
}

struct InsertTask {
    part_id: usize,
}

impl FlushTask {
    fn new(mem: Memory, schema: SchemaRef, part_id: usize) -> Self {
        FlushTask {
            mem,
            schema: schema.clone(),
            part_id,
        }
    }
}


impl Table {
    pub fn insert(&mut self, row: &Row) -> Result<()> {
        self.log.write(row)?;
        self.memory.insert(row)?;
        if self.memory.len() >= self.write_buffer_size {
            let mut empty = self.memory.new_empty();
            let immutable = mem::replace(&mut self.memory, empty);
            let task = FlushTask::new(
                immutable,
                self.schema.clone(),
                self.last_part_id + 1,
            );
            self.flush_queue_sender.send(task).unwrap();
            self.last_part_id += 1;
        }
        Ok(())
    }
}

fn flush_memtable(task: FlushTask, next: Sender<InsertTask>) {
    let record = task.mem.record_batch();
    let task = InsertTask {
        part_id: task.part_id,
    };

    /*
    1. наначаем part_id и кидаем в обработчик level0
    2. level0 обработчик ведёт
     */
}

struct Storage {
    tables: Vec<Table>,
}

impl Storage {
    fn insert(&mut self, table: &str) -> Result<()> {
        Ok(())
    }
}