use skiplist::OrderedSkipList;
use std::cmp::Ordering;
use datafusion::scalar::ScalarValue;
use arrow::record_batch::RecordBatch;
use arrow::datatypes::{Schema, SchemaRef, DataType};
use arrow::array::{BooleanArray, Int8Builder, Int8BufferBuilder, BooleanBufferBuilder, BooleanBuilder, ArrayRef, ArrayBuilder};
use super::error::Result;
use std::sync::Arc;

type Row = Vec<ScalarValue>;

#[derive(Clone)]
enum Order {
    Asc(usize),
    Desc(usize),
}

struct Memory {
    rows: skiplist::OrderedSkipList<Row>,
    cols: usize,
    len: usize,
}

impl Memory {
    fn new(order: Vec<Order>, cols: usize) -> Self {
        let mut m = Memory {
            rows: skiplist::OrderedSkipList::new(),
            cols,
            len: 0,
        };

        unsafe { m.rows.sort_by(sort_by(order)); }
        m
    }

    fn insert(&mut self, row: Row) {
        self.rows.insert(row);
        self.len += 1;
    }

    fn record_batch(&mut self, schema: SchemaRef) -> RecordBatch {
        let cols = (0..self.cols).map(|col_id| {
            match schema.fields()[col_id].data_type() {
                DataType::Boolean => {
                    // use buffer because standard is slower
                    let mut builder = BooleanBuilder::new(self.len);
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
                    let mut builder = Int8Builder::new(self.len);
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

        RecordBatch::try_new(schema.clone(), cols).unwrap()
    }

    fn clear(&mut self) {
        self.rows.clear();
        self.len = 0;
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

    #[test]
    fn test() {
        let mut mem = Memory::new(vec![Order::Asc(0), Order::Desc(1)], 2);
        mem.insert(vec![ScalarValue::Int8(Some(2)), ScalarValue::Int8(Some(1))]);
        mem.insert(vec![ScalarValue::Int8(Some(2)), ScalarValue::Int8(Some(2))]);
        mem.insert(vec![ScalarValue::Int8(Some(2)), ScalarValue::Int8(Some(3))]);
        mem.insert(vec![ScalarValue::Int8(Some(1)), ScalarValue::Int8(Some(2))]);
        mem.insert(vec![ScalarValue::Int8(Some(1)), ScalarValue::Int8(Some(3))]);
        mem.insert(vec![ScalarValue::Int8(Some(1)), ScalarValue::Int8(Some(1))]);

        for v in mem.rows.iter() {
            println!("{:?}", v);
        }
    }
}
