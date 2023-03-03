use std::cmp::Ordering;
use arrow2::compute::merge_sort::{merge_sort_slices, take_arrays};
use arrow2::array::{Int32Array, PrimitiveArray};
use arrow2::compute::merge_sort::{slices, SortOptions};
use arrow2::error::Result;

#[derive(Debug, Clone)]
/// usize(stream), usize(row_id), i32(val1), i32(val2)
struct ValsRow(usize, usize, i32, i32);

impl Ord for ValsRow {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.2, &self.3).cmp(&(other.2, &other.3))
    }
}

impl PartialOrd for ValsRow {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for ValsRow {
    fn eq(&self, other: &Self) -> bool {
        (self.2, &self.3) == (other.2, &other.3)
    }
}

impl Eq for ValsRow {}

fn streams_to_val_rows(streams: Vec<(usize, PrimitiveArray<i32>, PrimitiveArray<i32>)>) -> Vec<ValsRow> {
    let mut vals: Vec<ValsRow> = streams
        .into_iter()
        .map(|(stream, c1, c2)| c1.values_iter().zip(c2.values_iter()).enumerate()
            .map(|(row_id, (v1, v2))| ValsRow(stream, row_id, *v1, *v2))
            .collect::<Vec<ValsRow>>()).flatten().collect();

    vals
}

fn main() {
    //0
    let a1 = Int32Array::from_slice(&[1, 1, 2, 2]);
    let a2 = Int32Array::from_slice(&[1, 2, 2, 3]);
    let a3 = Int32Array::from_slice(&[1, 3, 7, 9]);

    //1
    let b1 = Int32Array::from_slice(&[1, 1, 2, 2]);
    let b2 = Int32Array::from_slice(&[1, 3, 1, 4]);
    let b3 = Int32Array::from_slice(&[2, 4, 6, 10]);

    //2
    let c1 = Int32Array::from_slice(&[2, 2, 3, 3]);
    let c2 = Int32Array::from_slice(&[1, 2, 1, 2]);
    let c3 = Int32Array::from_slice(&[5, 8, 11, 12]);

    let data_cols = vec![a3, b3, c3];
    let mut part1 = streams_to_val_rows(vec![(0, a1, a2), (2, c1, c2)]);
    part1.sort();
    let mut part2 = streams_to_val_rows(vec![(1, b1, b2)]);
    part2.sort();

    let mut f = [part1, part2].concat();
    f.sort();
    for (i, val) in f.iter().enumerate() {
        println!("{} {:?}\n", i + 1, val);
    }

    for vr in f.iter() {
        let v = data_cols[vr.0].value(vr.1);
        println!("{v}");
    }
}