use std::sync::mpsc;
use std::sync::mpsc::channel;
use std::sync::mpsc::Sender;
use std::sync::Arc;

use arrow2::array::Array;
use arrow2::array::growable::make_growable;
use arrow2::array::Int32Array;
use arrow2::array::Int64Array;
use arrow2::compute::merge_sort::merge_sort_slices;
use arrow2::compute::merge_sort::slices;
use arrow2::compute::merge_sort::take_arrays;
use arrow2::compute::merge_sort::SortOptions;
use arrow2::compute::sort::sort;
use futures::executor::block_on;

// creates a number of threads and waits for them to finish
fn main() {
    let a = Box::new(Int32Array::from_slice(&[2, 4, 6,7])) as Box<dyn Array>;
    let b = Box::new(Int32Array::from_slice(&[0, 1, 3])) as Box<dyn Array>;
    let mut growable = make_growable(&[a.as_ref(), b.as_ref()], false, a.len() + b.len());
    let slices = vec![
        (0, 0, 1),
        (0, 1, 1),
        (1, 2, 1),
        (0, 3, 1),
        (1, 0, 1),
        (1, 1, 1),
        (1, 2, 1),
    ];

    for slice in slices {
        growable.extend(slice.0, slice.1, slice.2);
    }

    println!("{:?}",growable.as_box())
}
