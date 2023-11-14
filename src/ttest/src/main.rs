
use std::sync::{Arc, mpsc};
use std::sync::mpsc::channel;
use std::sync::mpsc::Sender;
use arrow2::array::{Array, Int32Array, Int64Array};
use arrow2::compute::merge_sort::{merge_sort_slices, slices, SortOptions, take_arrays};
use arrow2::compute::sort::sort;

use futures::executor::block_on;

// creates a number of threads and waits for them to finish
fn main() {
    let a = Int32Array::from_slice(&[2, 4, 6]);
    let b = Int32Array::from_slice(&[0, 1, 3]);
    let slices = slices(&[(&[&a, &b], &SortOptions::default())]).unwrap();
    assert_eq!(slices, vec![(1, 0, 2), (0, 0, 1), (1, 2, 1), (0, 1, 2)]);
}
