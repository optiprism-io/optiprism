use std::cmp;
use std::cmp::Ordering;
use std::collections::btree_map::BTreeMap;
use std::collections::{BinaryHeap, VecDeque};
use std::fmt::{Debug, Display, Formatter};
use std::sync::atomic;
use std::sync::atomic::AtomicUsize;
use anyhow;
use rayon::prelude::*;

pub mod merge_parquet;

#[derive(Copy, Clone)]
struct Interval {
    stream: usize,
    from: i64,
    to: i64,
}

impl Display for Interval {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.stream < 99 {
            write!(f, "{}-{} (stream {})", self.from, self.to, self.stream)
        } else {
            write!(f, "{}-{} (mr {})", self.from, self.to, self.stream)
        }
    }
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

fn merge(id: usize, l: Interval, r: Interval) -> Vec<Interval> {
    let min = cmp::min(l.from, r.from);
    let max = cmp::max(l.to, r.to);

    println!("{id} {min} {max}");
    let md = (max - min) / 2;
    vec![
        Interval::new(id, min, md),
        Interval::new(id, md, max),
    ]
}

static MERGE_ID: AtomicUsize = AtomicUsize::new(99);

fn merge_all(ints: &mut Vec<(Interval, Interval)>) -> Vec<Interval> {
    ints.par_drain(..).map(|(l, r)| {
        let id = MERGE_ID.fetch_add(1, atomic::Ordering::SeqCst);
        merge(id, l, r)
    }).flatten().collect()
}

/*fn merge(ints: &[Interval]) -> Vec<Interval> {
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
}*/

fn main() -> anyhow::Result<()> {
    let s1 = vec![Interval::new(0, 1, 10), Interval::new(0, 13, 20)];
    let s2 = vec![Interval::new(1, 2, 4), Interval::new(1, 5, 8), Interval::new(1, 12, 20), Interval::new(1, 21, 22)];
    let s3 = vec![Interval::new(2, 11, 12), Interval::new(2, 13, 15), Interval::new(2, 16, 18)];

    let s1 = vec![Interval::new(0, 1, 10), Interval::new(0, 10, 20), Interval::new(0, 20, 30)];
    let s2 = vec![Interval::new(1, 1, 10), Interval::new(1, 10, 20), Interval::new(1, 20, 30)];
    let s3 = vec![Interval::new(2, 1, 10), Interval::new(2, 10, 20), Interval::new(2, 20, 30)];

    let threads = 1; // threads
    let mut streams = vec![
        s1.iter(),
        s2.iter(),
        s3.iter(),
    ];
    let mut merge_queue: Vec<(Interval, Interval)> = Vec::new();
    let mut sorter = BinaryHeap::<Interval>::new();
    let mut res: Vec<Interval> = Vec::new();
    for stream in streams.iter_mut() {
        if let Some(int) = stream.next() {
            sorter.push(*int);
        }
    }

    let mut l = 0;
    while let Some(int) = sorter.pop() {
        println!("in sorter {:?}",sorter);
        println!("in merge queue {:?}",merge_queue);
        println!("in result {:?}",res);
        if l > 100 {
            break;
        }
        l += 1;
        println!("popped {int} from sorter");
        if int.stream < 99 {
            if let Some(int) = streams[int.stream].next() {
                println!("pushed {int} to sorter");
                sorter.push(*int);
            } else {
                println!("stream {} is empty", int.stream);
            }
        }

        let mut to_merge = false;
        println!("peek {:?}", sorter.peek());
        if int.intersects(sorter.peek()) {
            let next_int = sorter.pop().unwrap();
            println!("pushed {int} and {next_int} to merge queue");
            merge_queue.push((int, next_int));
        } else if merge_queue.is_empty() {
            println!("result {int}");
            res.push(int);
        } else {
            println!("push {int} back to sorter and trigger merge");
            sorter.push(int);
            to_merge = true;
        }

        if merge_queue.len() == threads || to_merge {
            if merge_queue.len() == threads {
                println!("merge queue is full");
            } else {
                println!("to merge");
            }
            println!("merge queue {:?}", merge_queue);
            let mrs = merge_all(&mut merge_queue);
            println!("merge result {:?}", mrs);
            for mr in mrs.into_iter() {
                sorter.push(mr);
            }
        }
    }

    println!("no more intervals");

    if !merge_queue.is_empty() {
        println!("final merge of queue {:?}", merge_queue);
        let mut mrs = merge_all(&mut merge_queue);
        mrs.sort_by(|a, b| b.cmp(a));
        println!("sorted merge result: {:?}", mrs);
        res.append(&mut mrs);
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


/*
sorter: [#0 (1,10), #1 (1,10), #2 (1,10)]
merge [#0 (1,10), #1 (1,10), #2 (1,10)] to (1,10)
sorter: [#0 (10,20), #1 (10,20), #2 (10,20)]
merge [#0 (10,20), #1 (10,20), #2 (10,20)] to (10,20)
merge of queue: [[#0 (1,10), #1 (1,10), #2 (1,10)], [#0 (10,20), #1 (10,20), #2 (10,20)]]
0 #99 (1,4)
0 #99 (10,13)
1 #99 (13,16)
1 #99 (4,7)
2 #99 (16,20)
2 #99 (7,10)
sorter: [#99 (1,4), #0 (20,30), #2 (20,30), #1 (20,30)]
sorter: [#99 (4,7), #2 (20,30), #1 (20,30), #0 (20,30)]
sorter: [#99 (7,10), #1 (20,30), #0 (20,30), #2 (20,30)]
sorter: [#99 (10,13), #0 (20,30), #2 (20,30), #1 (20,30)]
sorter: [#99 (13,16), #2 (20,30), #1 (20,30), #0 (20,30)]
sorter: [#99 (16,20), #1 (20,30), #0 (20,30), #2 (20,30)]
merge [#99 (16,20), #0 (20,30), #1 (20,30), #2 (20,30)] to (16,30)
final merge
0 #99 (16,19)
1 #99 (19,22)
2 #99 (22,25)
3 #99 (25,30)
sorter: [#99 (16,19)]
sorter: [#99 (19,22)]
sorter: [#99 (22,25)]
sorter: [#99 (25,30)]

final
[#99 (1,4), #99 (4,7), #99 (7,10), #99 (10,13), #99 (13,16), #99 (16,19), #99 (19,22), #99 (22,25), #99 (25,30)]
 */