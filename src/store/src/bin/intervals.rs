use std::cmp::Ordering;
use std::collections::btree_map::BTreeMap;
use std::collections::{BinaryHeap, VecDeque};
use std::fmt::{Debug, Formatter};
use anyhow;

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

fn main() -> anyhow::Result<()> {
    let s1 = vec![Interval::new(0, 1, 10), Interval::new(0, 13, 20)];
    let s2 = vec![Interval::new(1, 2, 4), Interval::new(1, 5, 8), Interval::new(1, 12, 20), Interval::new(1, 21, 22)];
    let s3 = vec![Interval::new(2, 11, 12), Interval::new(2, 13, 15), Interval::new(2, 16, 18)];

    let s1 = vec![Interval::new(0, 1, 10), Interval::new(0, 10, 20), Interval::new(0, 20, 30)];
    let s2 = vec![Interval::new(0, 1, 10), Interval::new(0, 10, 20), Interval::new(0, 20, 30)];
    let s3 = vec![Interval::new(0, 1, 10), Interval::new(0, 10, 20), Interval::new(0, 20, 30)];
    let mut streams = vec![
        s1.iter(),
        s2.iter(),
        s3.iter(),
    ];
    let mut merge_result: BinaryHeap<Interval> = BinaryHeap::new();
    let mut sorter: BinaryHeap<Interval> = BinaryHeap::new();
    let mut res = vec![];
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
            break;
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
                        pop = true;
                    } else {
                        break;
                    }
                }
            }

            /*if pop {
                pop = false;
                sorter.pop_front();
            }*/
        }

        if out.len() == 1 {
            res.push(out[0]);
        } else {
            println!("merge {:?} to ({min},{max})", out);
            let size = (max - min) / out.len() as i64;
            for (i, iter) in out.iter().enumerate() {
                let i = i as i64;
                let a = min + i * size;
                let mut b = a + size - 1;
                if i == out.len() as i64 - 1 && b != max {
                    b = max;
                }
                println!("{} {:?}", i, Interval::new(99, a, b));
                merge_result.push(Interval::new(99, a, b));
            }
            println!("{} {} {}", (max - min), out.len(), size);
            /*for i in (min..max).step_by(size as usize) {
                println!("{} - {:?}", i, Interval::new(99, i, i + size));
                merge_result.push(Interval::new(99, i, i + size));
            }*/
        }
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