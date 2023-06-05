use std::collections::VecDeque;


#[derive(Debug)]
struct State {
    last_value: Option<usize>,
    spans: Vec<usize>,
    buf: Vec<Vec<usize>>,
}

impl State {
    pub fn new() -> Self {
        Self {
            last_value: None,
            spans: vec![0],
            buf: Vec::new(),
        }
    }

    pub fn push(&mut self, batch: Vec<usize>) -> Option<(Vec<Vec<usize>>,Vec<usize>)> {
        self.buf.push(batch.clone());
        let mut take = false;
        for (idx, v) in batch.into_iter().enumerate() {
            if self.last_value.is_none() {
                self.last_value = Some(v);
            }

            if self.last_value != Some(v) {
                self.spans.push(0);
                take = true;
            }
            let i = self.spans.len() - 1;
            self.spans[i] += 1;
            self.last_value = Some(v);
        }

        if self.buf.len() > 1 && take {
            let mut take_batches = self.buf.drain(..self.buf.len() - 1).collect::<Vec<Vec<usize>>>();
            take_batches.push(self.buf.last().unwrap().to_owned());
            let take_spans = self.spans.drain(..self.spans.len() - 1).collect::<Vec<usize>>();

            println!("take batches {:?} spans {:?}", take_batches, take_spans);
            return Some((take_batches,take_spans));
        }
        None
    }
}

pub fn run(mut queue: VecDeque<Vec<usize>>) {
    let mut last_value: Option<usize> = None;
    let mut offset = 0;
    let mut spans: VecDeque<usize> = VecDeque::from(vec![0]);
    let mut buf: Vec<Vec<usize>> = Vec::new();
    let mut take = false;
    // let mut batch_ids:Vec<_> = vec![];
    let mut batch_id = 0;

    while let Some(batch) = queue.pop_front() {
        buf.push(batch.clone());
        for (idx, v) in batch.into_iter().enumerate() {
            if last_value.is_none() {
                last_value = Some(v);
            }

            if last_value != Some(v) {
                spans.push_back(0);
                take = true;
            }
            let spanidx = spans.len() - 1;
            spans[spanidx] += 1;
            last_value = Some(v);
        }

        // println!("{:?} {:?} ", buf, spans);
        // println!("{}", buf.len());
        if buf.len() > 1 && take {
            take = false;
            let mut take_batches = buf.drain(..buf.len() - 1).collect::<Vec<Vec<usize>>>();
            take_batches.push(buf.last().unwrap().to_owned());
            let take_spans = spans.drain(..spans.len() - 1).collect::<Vec<usize>>();

            // println!("take batches {:?} spans {:?}", take_batches, take_spans);
            println!("take batches {:?} spans {:?}", take_batches, take_spans);
        }
        batch_id += 1;
        /*        buf.push(batch.clone());
                for (idx, v) in batch.into_iter().enumerate() {
                    if last_value.is_none() {
                        last_value = Some(v);
                    }
                    if last_value != Some(v) {
                        spans.push_back(0);
                    }
                    let spanidx = spans.len() - 1;
                    // offset += 1;
                    spans[spanidx] += 1;
                    last_value = Some(v);
                }
                if spans.len() > 1 {
                    // println!("spans before {:?}", spans);
                    let take = spans.len() - 2;
                    let spans_res = spans.drain(..take).collect::<Vec<usize>>();
                    let buf_res = buf.drain(..buf.len() - 1).collect::<Vec<Vec<usize>>>();
                    println!("{:?} {:?}", buf_res, spans_res);
                }*/
    }

    /*while let Some(batch) = queue.pop_front() {
        buf.push(batch.clone());
        for (idx, v) in batch.into_iter().enumerate() {
            if last_value.is_none() {
                last_value = Some(v);
            }
            if last_value != Some(v) {
                spans.push_back(0);
            }
            let spanidx = spans.len() - 1;
            // offset += 1;
            spans[spanidx] += 1;
            last_value = Some(v);
        }
        if spans.len() > 1 {
            // println!("spans before {:?}", spans);
            let take = spans.len() - 2;
            let spans_res = spans.drain(..take).collect::<Vec<usize>>();
            let buf_res = buf.drain(..buf.len() - 1).collect::<Vec<Vec<usize>>>();
            println!("{:?} {:?}", buf_res, spans_res);
        }
    }*/
}

fn main() {
    let vv = vec![
        vec![0, 0, 0, 0],
        vec![1, 1, 1, 1, 2, 2, 2, 2, 2], // 0-1, 4 4
        vec![2, 3, 3, 3, 4, 4, 4, 5, 5], // 1-2, 6 3 3
        vec![6], // 2-3, 2
        vec![6],
        vec![6],
        vec![7, 7, 7], // 3-6, 3
        vec![8, 8, 8], // 6-7, 3
        // 3
    ];

    println!("PUSH");
    let mut state = State::new();
    for v in vv.clone() {
        let res=state.push(v);
        println!("res: {:?}",res);
    }

    println!("PULL");
    run(VecDeque::from(vv));
    // let mut state = State2::new(vv);
    // state.run();
}