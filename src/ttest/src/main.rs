struct Funnel {
    seq: Vec<isize>,
    current_partition: isize,
    ptr: usize,
    partition_size: usize,
}

impl Funnel {
    fn new(seq: Vec<isize>) -> Self {
        Self {
            seq,
            current_partition: 0,
            ptr: 0,
            partition_size: 0,
        }
    }
    fn push(&mut self, batch: [Vec<isize>; 2]) -> Option<Vec<bool>> {
        let [keys, values] = batch;
        let res: Vec<bool> = keys
            .into_iter()
            .zip(values)
            .filter_map(|(key, value)| {
                let mut maybe_full = None;
                if key != self.current_partition && self.partition_size != 0 {
                    if self.partition_size >= self.seq.len() {
                        maybe_full = Some(self.ptr == self.seq.len());
                    }
                    self.partition_size = 0;
                    self.ptr = 0;
                }
                self.current_partition = key;
                self.partition_size += 1;
                if self.ptr < self.seq.len() && self.seq[self.ptr] == value {
                    self.ptr += 1;
                }
                maybe_full
            })
            .collect();
        if res.is_empty() { None } else { Some(res) }
    }
    fn finalize(&self) -> bool {
        if self.partition_size >= self.seq.len() {
            self.ptr == self.seq.len()
        } else {
            false
        }
    }
}

fn main() {}

#[cfg(test)]
mod tests {
    use crate::Funnel;

    #[test]
    fn it_works() {
        let mut funnel = Funnel::new(vec![1, 2, 3]);
        let b0 = [vec![1, 1, 1, 2, 2, 2, 3], vec![1, 2, 3, 1, 1, 1, 1]];
        let res = funnel.push(b0);
        assert_eq!(Some(vec![true, false]), res);
        let b1 = [vec![1, 1, 1, 1, 2, 2], vec![1, 2, 3, 3, 1, 2]];
        let res = funnel.push(b1);
        assert_eq!(Some(vec![true]), res);
        let b2 = [vec![2, 3, 3, 3, 3, 3, 4], vec![3, 1, 2, 1, 4, 3, 1]];
        let res = funnel.push(b2);
        assert_eq!(Some(vec![true, true]), res);
        let b3 = [vec![4], vec![2]];
        let res = funnel.push(b3);
        assert_eq!(None, res);
        let res = funnel.finalize();
        assert_eq!(false, res);
    }

    #[test]
    fn it_works2() {
        let mut funnel = Funnel::new(vec![1, 2, 3]);
        let b0 = [vec![1, 1, 1, 1, 1, 1, 3], vec![1, 2, 1, 3, 1, 1, 1]];
        let res = funnel.push(b0);
        println!("{:?}", res);
    }
}
