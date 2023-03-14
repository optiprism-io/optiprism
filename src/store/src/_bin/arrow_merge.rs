use std::cmp::Ordering;
use std::{mem, ptr};
use std::ptr::addr_of_mut;
use std::sync::Arc;
use arrow::ffi::ArrowArray;
use arrow_array::{ArrayRef, make_array};
use arrow2::compute::merge_sort::{merge_sort_slices, take_arrays};
use arrow2::array::{Array, BinaryArray, BooleanArray, Int32Array, ListArray, MutableListArray, MutablePrimitiveArray, PrimitiveArray, Utf8Array};
use arrow2::compute::merge_sort::{slices, SortOptions};
use arrow2::datatypes::Field;
use arrow2::ffi;
use store::error::Result;
use arrow2::array::TryExtend;

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

fn export2(array: Box<dyn Array>) -> (ffi::ArrowArray, ffi::ArrowSchema) {
    // importing an array requires an associated field so that the consumer knows its datatype.
    // Thus, we need to export both
    let field = Field::new("a", array.data_type().clone(), true);
    (
        ffi::export_array_to_c(array),
        ffi::export_field_to_c(&field),
    )
}

pub use arrow_schema::ffi::{FFI_ArrowSchema, Flags};
pub use arrow_data::ffi::FFI_ArrowArray;
use arrow2::ffi::ArrowSchema;




fn main() {
    let arr1 = {
        let arr2 = Box::new(Int32Array::from(&[Some(1), Some(1), Some(2), None])) as Box<dyn Array>;
        let dt = arr2.data_type().to_owned();
        arrow2_to_arrow1(arr2, Field::new("a", dt, true)).unwrap()
    };
    println!("{:?}", arr1);

    let arr2 = {
        let arr2 = Box::new(BooleanArray::from(&[Some(true), Some(false), Some(true), None])) as Box<dyn Array>;
        let dt = arr2.data_type().to_owned();
        arrow2_to_arrow1(arr2, Field::new("a", dt, true)).unwrap()
    };
    println!("{:?}", arr2);

    let arr3 = {
        let arr2 = Box::new(Utf8Array::<i32>::from(&[Some("a"), Some("b"), Some("c"), None])) as Box<dyn Array>;
        let dt = arr2.data_type().to_owned();
        arrow2_to_arrow1(arr2, Field::new("a", dt, true)).unwrap()
    };
    println!("{:?}", arr3);

    let arr4 = {
        let arr2 = Box::new(Utf8Array::<i64>::from(&[Some("a"), Some("b"), Some("c"), None])) as Box<dyn Array>;
        let dt = arr2.data_type().to_owned();
        arrow2_to_arrow1(arr2, Field::new("a", dt, true)).unwrap()
    };
    println!("{:?}", arr4);

    let arr5 = {
        let arr2 = Box::new(BinaryArray::<i32>::from([Some([1, 2].as_ref()), None, Some([3].as_ref())]));
        let dt = arr2.data_type().to_owned();
        arrow2_to_arrow1(arr2, Field::new("a", dt, true)).unwrap()
    };
    println!("{:?}", arr5);

    let arr6 = {
        let data = vec![
            Some(vec![Some(1i32), Some(2), Some(3)]),
            Some(vec![Some(4), Some(5)]),
            Some(vec![Some(6i32), Some(7), Some(8)]),
        ];
        let mut array = MutableListArray::<i32, MutablePrimitiveArray<i32>>::new();
        array.try_extend(data).unwrap();
        let arr2: ListArray<i32> = array.into();

        let dt = arr2.data_type().to_owned();
        arrow2_to_arrow1(Box::new(arr2), Field::new("a", dt, true)).unwrap()
    };
    println!("{:?}", arr6);
    return;
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