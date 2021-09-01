use skiplist::OrderedSkipList;
use std::cmp::Ordering;
use datafusion::scalar::ScalarValue;

type Row = Vec<ScalarValue>;

#[derive(Clone)]
enum Order {
    Asc(usize),
    Desc(usize),
}

struct Memory {
    rows: skiplist::OrderedSkipList<Row>,
}

impl Memory {
    fn new(order: Vec<Order>) -> Self {
        let mut m = Memory {
            rows: skiplist::OrderedSkipList::new(),
        };

        unsafe { m.rows.sort_by(sort_by(order)); }

        m
    }

    fn insert(&mut self, row: Row) {
        self.rows.insert(row);
    }
}

fn sort_by<'a>(order: Vec<Order>) -> impl Fn(&Row, &Row) -> Ordering {
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
        let mut mem = Memory::new(vec![Order::Asc(0), Order::Desc(1)]);
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

/*
[Int8(1), Int8(1)]
[Int8(1), Int8(2)]
[Int8(1), Int8(3)]
[Int8(2), Int8(1)]
[Int8(2), Int8(2)]
[Int8(2), Int8(3)]
 */