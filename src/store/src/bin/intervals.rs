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
}

impl Debug for Interval {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "#{} ({},{})", self.stream, self.from, self.to)
    }
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
        let mut b = a + size ;
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
    let s2 = vec![Interval::new(0, 1, 10), Interval::new(0, 10, 20), Interval::new(0, 20, 30)];
    let s3 = vec![Interval::new(0, 1, 10), Interval::new(0, 10, 20), Interval::new(0, 20, 30)];

    let threads = 2; // threads
    let mut streams = vec![
        s1.iter(),
        s2.iter(),
        s3.iter(),
    ];
    let mut merge_queue: Vec<Vec<Interval>> = Vec::new();
    let mut merge_result = BinaryHeap::<Interval>::new();
    let mut sorter = BinaryHeap::<Interval>::new();
    let mut res = vec![];
    let mut l = 0;
    loop {
        for (stream_idx, stream) in streams.iter_mut().enumerate() {
            let mut in_sorter = false;
            for i in sorter.iter() {
                if i.stream == stream_idx {
                    in_sorter = true;
                    break;
                }
            }

            if !in_sorter {
                match stream.next() {
                    Some(int) => sorter.push(Interval::new(stream_idx, int.from, int.to)),
                    _ => {}
                }
            }
        }

        if let Some(int) = merge_result.pop() {
            sorter.push(int);
        }
        if sorter.is_empty() {
            if merge_queue.is_empty() {
                break;
            }

            println!("final merge");
            let res: Vec<Vec<Interval>> = merge_queue.par_drain(..).map(|v| merge(&v)).collect();
            res.into_iter().for_each(|v| v.into_iter().for_each(|v| merge_result.push(v)));
            if let Some(int) = merge_result.pop() {
                sorter.push(int);
            }
        }
        println!("sorter: {:?}", sorter);
        let mut out = Vec::new();
        let first = sorter.pop().unwrap();
        let (mut min, mut max) = (first.from, first.to);
        out.push(first);

        while !sorter.is_empty() {
            let mut pop = false;
            match sorter.peek() {
                None => break,
                Some(int) => {
                    if int.from >= min && int.from <= max {
                        out.push(*int);
                        if int.to > max {
                            max = int.to;
                        }
                        sorter.pop();
                    } else {
                        break;
                    }
                }
            }
        }

        if out.len() == 1 {
            res.push(out[0]);
        } else {
            println!("merge {:?} to ({min},{max})", out);
            merge_queue.push(out);
            // it is time to merge
            if merge_queue.len() == threads {
                println!("merge of queue: {:?}", merge_queue);
                let mr: Vec<Vec<Interval>> = merge_queue.par_drain(..).map(|v| merge(&v)).collect();
                mr.into_iter().for_each(|v| v.into_iter().for_each(|v| merge_result.push(v)));

            }
        }
        l += 1;
    }

    println!("\nfinal\n{:?}", res);

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
