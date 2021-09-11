use skiplist::OrderedSkipList;
use std::cmp::Ordering;
use datafusion::scalar::ScalarValue;
use arrow::record_batch::RecordBatch;
use arrow::datatypes::{Schema, SchemaRef, DataType};
use arrow::array::{BooleanArray, Int8Builder, Int8BufferBuilder, BooleanBufferBuilder, BooleanBuilder, ArrayRef, ArrayBuilder};
use super::error::Result;
use std::sync::Arc;
use crate::storage::storage::{Row, Order};

pub struct Memory {
    rows: skiplist::OrderedSkipList<Row>,
    order: Vec<Order>,
    schema: SchemaRef,
}

impl Memory {
    fn new(order: Vec<Order>, schema: SchemaRef) -> Self {
        let mut m = Memory {
            rows: skiplist::OrderedSkipList::new(),
            schema: schema.clone(),
            order: order.clone(),
        };

        unsafe { m.rows.sort_by(sort_by(order.clone())); }
        m
    }

    pub fn new_empty(&self) -> Self {
        let mut m = Memory {
            rows: skiplist::OrderedSkipList::new(),
            order: self.order.clone(),
            schema: self.schema.clone(),
        };

        unsafe { m.rows.sort_by(sort_by(self.order.clone())); }
        m
    }

    pub fn insert(&mut self, row: &Row) -> Result<()> {
        self.rows.insert(row.clone());
        Ok(())
    }

    pub fn record_batch(&self) -> RecordBatch {
        let cols = self.schema.fields().iter().enumerate().map(|(col_id, field)| {
            match field.data_type() {
                DataType::Boolean => {
                    // use buffer because standard is slower
                    let mut builder = BooleanBuilder::new(self.rows.len());
                    for row in self.rows.iter() {
                        // columns can't be deleted within current memory instance, so we can check column presence like this
                        if col_id < row.len() - 1 {
                            let v = if let ScalarValue::Boolean(v) = row[col_id] { v } else { panic!("error casting to Boolean") };
                            builder.append_option(v).unwrap();
                        } else {
                            builder.append_null();
                        }
                    }
                    Arc::new(builder.finish()) as ArrayRef
                }
                DataType::Int8 => {
                    let mut builder = Int8Builder::new(self.rows.len());
                    for row in self.rows.iter() {
                        if col_id < row.len() - 1 {
                            let v = if let ScalarValue::Int8(v) = row[col_id] { v } else { panic!("error casting to Int8") };
                            builder.append_option(v).unwrap();
                        } else {
                            builder.append_null();
                        }
                    }
                    Arc::new(builder.finish()) as ArrayRef
                }
                _ => unimplemented!()
            }
        }).collect();

        RecordBatch::try_new(self.schema.clone(), cols).unwrap()
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    fn clear(&mut self) {
        self.rows.clear();
    }
}

fn sort_by(order: Vec<Order>) -> impl Fn(&Row, &Row) -> Ordering {
    move |a, b| {
        let mut iter = order.iter();
        let mut res = {
            match iter.next().unwrap() {
                Order::Asc(col_id) => a[*col_id].partial_cmp(&b[*col_id]).unwrap(),
                Order::Desc(col_id) => b[*col_id].partial_cmp(&a[*col_id]).unwrap(),
            }
        };

        for order in iter {
            res = res.then(match order {
                Order::Asc(col_id) => a[*col_id].partial_cmp(&b[*col_id]).unwrap(),
                Order::Desc(col_id) => b[*col_id].partial_cmp(&a[*col_id]).unwrap(),
            })
        }
        res
    }
}

mod tests {
    use crate::storage::memory::{Memory, Order};
    use datafusion::scalar::ScalarValue;
    use arrow::datatypes::{Schema, Field, DataType};
    use std::sync::Arc;

    #[test]
    fn test() {
        let schema = Schema::new(vec![Field::new("1", DataType::Int8, true), Field::new("2", DataType::Int8, true)]);
        let mut mem = Memory::new(vec![Order::Asc(0), Order::Desc(1)], Arc::new(schema));
        mem.insert(&vec![ScalarValue::Int8(Some(2)), ScalarValue::Int8(Some(1))]);
        mem.insert(&vec![ScalarValue::Int8(Some(2)), ScalarValue::Int8(Some(2))]);
        mem.insert(&vec![ScalarValue::Int8(Some(2)), ScalarValue::Int8(Some(3))]);
        mem.insert(&vec![ScalarValue::Int8(Some(1)), ScalarValue::Int8(Some(2))]);
        mem.insert(&vec![ScalarValue::Int8(Some(1)), ScalarValue::Int8(Some(3))]);
        mem.insert(&vec![ScalarValue::Int8(Some(1)), ScalarValue::Int8(Some(1))]);

        for v in mem.rows.iter() {
            println!("{:?}", v);
        }
    }
}
