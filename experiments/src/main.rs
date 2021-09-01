use skiplist::OrderedSkipList;
use std::cmp::Ordering;


#[derive(Debug, PartialEq, Eq)]
struct Value {
    a: usize,
    b: usize,
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> Ordering {
        self.a.cmp(&other.a).then(other.b.cmp(&self.b))
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.a.cmp(&other.a).then(other.b.cmp(&self.b)))
    }
}

fn main() {
    let a = vec![1, 2, 3];

    let mut iter = a.iter();
    iter.next();
    for i in iter {
        println!("{}", i);
    }
    let mut sl: OrderedSkipList<Value> = OrderedSkipList::new();
    unsafe { sl.sort_by(|a, b| a.a.cmp(&b.a).then(b.b.cmp(&a.b))) }
    sl.insert(Value { a: 2, b: 1 });
    sl.insert(Value { a: 2, b: 2 });
    sl.insert(Value { a: 2, b: 3 });
    sl.insert(Value { a: 1, b: 2 });
    sl.insert(Value { a: 1, b: 3 });
    sl.insert(Value { a: 1, b: 1 });
    for v in &sl {
        println!("{:?}", v);
    }
}
