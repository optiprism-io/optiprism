use std::cmp::Ordering;
use std::collections::btree_map::BTreeMap;
use std::collections::{BinaryHeap, VecDeque};
use std::fmt::{Debug, Formatter};
use anyhow;
use rayon::prelude::*;

pub mod merge_parquet;

#[derive(Copy, Clone)]
struct Interval {
    stream: usize,
    from: i64,
    to: i64,
}

impl Interval {
    pub fn new(stream: usize, from: i64, to: i64) -> Self {
        Self { stream, from, to }
    }
    pub fn intersects(&self, other: Option<&Interval>) -> bool {
        if other.is_none() {
            false
        } else {
            let other = other.unwrap();
            self.stream != other.stream && self.from <= other.to && self.to >= other.from
        }
    }
}

impl Debug for Interval {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "#{} ({},{})", self.stream, self.from, self.to)
    }
}

fn merge_interval(ints: &[Interval]) -> Interval {
    let mut min = i64::MAX;
    let mut max = i64::MIN;
    for int in ints.iter() {
        if int.from <= min {
            min = int.from;
        }
        if int.to >= max {
            max = int.to;
        }
    }

    Interval::new(99, min, max)
}

fn merge(ints: &[Interval]) -> Vec<Interval> {
    let mut min = i64::MAX;
    let mut max = i64::MIN;
    for int in ints.iter() {
        if int.from <= min {
            min = int.from;
        }
        if int.to >= max {
            max = int.to;
        }
    }
    let mut res = Vec::new();
    let size = (max - min) / ints.len() as i64;
    for (i, _) in ints.iter().enumerate() {
        let i = i as i64;
        let a = min + i * size;
        let mut b = a + size;
        if i == ints.len() as i64 - 1 && b != max {
            b = max;
        }
        println!("{} {:?}", i, Interval::new(99, a, b));
        res.push(Interval::new(99, a, b));
    }

    res
}

fn main() -> anyhow::Result<()> {
    let s1 = vec![Interval::new(0, 1, 10), Interval::new(0, 13, 20)];
    let s2 = vec![Interval::new(1, 2, 4), Interval::new(1, 5, 8), Interval::new(1, 12, 20), Interval::new(1, 21, 22)];
    let s3 = vec![Interval::new(2, 11, 12), Interval::new(2, 13, 15), Interval::new(2, 16, 18)];

    let s1 = vec![Interval::new(0, 1, 10), Interval::new(0, 10, 20), Interval::new(0, 20, 30)];
    let s2 = vec![Interval::new(1, 1, 10), Interval::new(1, 10, 20), Interval::new(1, 20, 30)];
    let s3 = vec![Interval::new(2, 1, 10), Interval::new(2, 10, 20), Interval::new(2, 20, 30)];

    let s1 = vec![Interval::new(0, 1, 10), Interval::new(0, 11, 20), Interval::new(0, 21, 30)];
    let s2 = vec![Interval::new(1, 1, 10), Interval::new(1, 11, 20), Interval::new(1, 21, 30)];
    let s3 = vec![Interval::new(2, 1, 10), Interval::new(2, 11, 20), Interval::new(2, 21, 30)];

    let s1 = vec![Interval::new(0, 1, 10), Interval::new(0, 10, 20), Interval::new(0, 20, 30), Interval::new(0, 50, 60)];
    let s2 = vec![Interval::new(1, 3, 5), Interval::new(1, 6, 8), Interval::new(1, 40, 45), Interval::new(1, 61, 69)];
    let s3 = vec![Interval::new(2, 20, 22), Interval::new(2, 22, 23), Interval::new(2, 23, 24), Interval::new(2, 24, 25), Interval::new(2, 60, 61), Interval::new(2, 80, 81)];

    println!("s1: {:?}", s1);
    println!("s2: {:?}", s2);
    println!("s3: {:?}", s3);
    let threads = 4; // threads
    let mut streams = vec![
        s1.iter(),
        s2.iter(),
        s3.iter(),
    ];


    let mut sorter = BinaryHeap::<Interval>::new();
    for stream in streams.iter_mut() {
        if let Some(int) = stream.next() {
            sorter.push(*int);
        }
    }

    let mut res = vec![];
    let mut mq = vec![];
    while let Some(int) = sorter.pop() {
        println!("pop {:?}", int);
        if let Some(next) = streams[int.stream].next() {
            println!("pop next {:?}", next);
            sorter.push(*next);
        }

        mq.push(int);
        let mut ms = vec![];
        while merge_interval(&mq).intersects(sorter.peek()) {
            if ms.contains(&sorter.peek().unwrap().stream) {
                break;
            }
            println!("{:?} intersects with {:?}", int, sorter.peek());
            ms.push(sorter.peek().unwrap().stream);
            let next = sorter.pop().unwrap();
            if let Some(next) = streams[next.stream].next() {
                println!("pop next {:?}", next);
                sorter.push(*next);
            }
            mq.push(next);
        }

        if mq.len() > 1 {
            println!("merge queue {:?}", mq);
            let mut mr = merge(&mq);
            println!("merge result {:?}", mr);
            res.append(&mut mr);
            mq.clear();
            println!("result {:?}", res);
        } else {
            println!("push to result {:?}", int);
            res.push(int);
            println!("result {:?}", res);
        }
        /*println!("pop {:?}", int);

        let mut buf = vec![int];

        while int.intersects(sorter.peek()) {
            println!("intersects with {:?}", sorter.peek());
            buf.push(sorter.pop().unwrap());
        }

        for int in buf.iter() {
            if let Some(next) = streams[int.stream].next() {
                println!("pop next {:?}", next);
                sorter.push(*next);
            }
        }

        if buf.len() == 1 {
            println!("{:?}", buf[0]);
            res.push(buf[0]);
            continue;
        }

        println!("merge {:?}", buf);
        for int in merge(&buf).into_iter() {
            println!("{:?}", int);
            res.push(int);
        }*/
    }

    println!("final: {:?}", res);
    Ok(())
}

impl Eq for Interval {}

impl PartialOrd for Interval {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(other.cmp(self))
    }

    fn lt(&self, other: &Self) -> bool {
        other.from < self.from
    }
    #[inline]
    fn le(&self, other: &Self) -> bool {
        other.from <= self.from
    }
    #[inline]
    fn gt(&self, other: &Self) -> bool {
        other.from > self.from
    }
    #[inline]
    fn ge(&self, other: &Self) -> bool {
        other.from >= self.from
    }
}

impl Ord for Interval {
    fn cmp(&self, other: &Self) -> Ordering {
        other.from.cmp(&self.from)
    }
}

impl PartialEq for Interval {
    fn eq(&self, other: &Self) -> bool {
        self.from == other.from && self.to == other.to
    }
}
