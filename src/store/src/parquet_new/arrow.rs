use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::mem;
use std::slice::Iter;
use bitmaps::Bitmap;
use crate::error::{Result, StoreError};
use arrow2::array::{Array, BinaryArray, Int32Array, Int64Array, MutablePrimitiveArray, PrimitiveArray};
use arrow2::compute::merge_sort::MergeSlice;
use arrow2::datatypes::DataType;
use arrow2::types::{NativeType, PrimitiveType};
use parquet2::schema::types::{ParquetType, PhysicalType};
use parquet2::write::Compressor;
use crate::parquet::Value;

#[derive(Debug)]
pub struct MergedArrowChunk {
    pub arrs: Vec<Box<dyn Array>>,
    pub reorder: Vec<usize>,
}

impl MergedArrowChunk {
    pub fn new(arrs: Vec<Box<dyn Array>>, reorder: Vec<usize>) -> Self {
        Self {
            arrs,
            reorder,
        }
    }
}

#[derive(Debug)]
pub struct ArrowChunk {
    pub stream: usize,
    pub arrs: Vec<Box<dyn Array>>,
}

macro_rules! value {
    ($arr:expr, $ty:ty, $n:expr) => {{
        let v = $arr.as_any().downcast_ref::<$ty>().unwrap();
        Value::from(v.value($n))
    }};
}

impl ArrowChunk {
    pub fn new(arrs: Vec<Box<dyn Array>>, stream: usize) -> Self {
        Self {
            stream,
            arrs,
        }
    }

    pub fn len(&self) -> usize {
        self.arrs[0].len()
    }
}

pub struct TwoColMergeRow<A, B>(usize, A, B);

impl<A, B> Ord for TwoColMergeRow<A, B> where A: Ord, B: Ord {
    fn cmp(&self, other: &Self) -> Ordering {
        (&self.1, &self.2).cmp(&(&other.1, &other.2)).reverse()
    }
}

impl<A, B> PartialOrd for TwoColMergeRow<A, B> where A: Ord, B: Ord {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<A, B> PartialEq for TwoColMergeRow<A, B> where A: Eq, B: Eq {
    fn eq(&self, other: &Self) -> bool {
        (&self.1, &self.2) == (&other.1, &other.2)
    }
}

impl<A, B> Eq for TwoColMergeRow<A, B> where A: Eq, B: Eq {}

pub fn merge_two_primitives<T1: NativeType + Ord, T2: NativeType + Ord>(mut chunks: Vec<ArrowChunk>, array_size: usize) -> Result<Vec<MergedArrowChunk>> {
    let mut arr_iters = chunks.iter().map(|row| {
        let arr1 = row.arrs[0].as_any().downcast_ref::<PrimitiveArray<T1>>().unwrap().values_iter();
        let arr2 = row.arrs[1].as_any().downcast_ref::<PrimitiveArray<T2>>().unwrap().values_iter();
        (arr1, arr2)
    }).collect::<Vec<_>>();

    let mut sort = BinaryHeap::<TwoColMergeRow<T1, T2>>::with_capacity(array_size);

    let mut res = vec![];
    let mut out_col1 = Vec::with_capacity(array_size);
    let mut out_col2 = Vec::with_capacity(array_size);
    let mut order = Vec::with_capacity(array_size);

    for row_id in 0..chunks.len() {
        let mr = TwoColMergeRow(
            row_id,
            *arr_iters[row_id].0.next().unwrap(),
            *arr_iters[row_id].1.next().unwrap(),
        );
        sort.push(mr);
    }

    while let Some(TwoColMergeRow(row_idx, v1, v2)) = sort.pop() {
        out_col1.push(v1);
        out_col2.push(v2);
        order.push(chunks[row_idx].stream);
        match arr_iters[row_idx].0.next() {
            Some(v1) => {
                let v2 = arr_iters[row_idx].1.next().unwrap();

                let mr = TwoColMergeRow(
                    row_idx,
                    *v1,
                    *v2,
                );

                sort.push(mr);
            }
            None => {}
        }

        if out_col1.len() >= array_size {
            let out = vec![
                PrimitiveArray::<T1>::from_vec(out_col1.drain(..).collect()).boxed(),
                PrimitiveArray::<T2>::from_vec(out_col2.drain(..).collect()).boxed(),
            ];
            let arr_order = order.drain(..).collect();
            res.push(MergedArrowChunk::new(out , arr_order));
        }
    }

    if !out_col1.is_empty() {
        let out = vec![
            PrimitiveArray::<T1>::from_vec(out_col1.drain(..).collect()).boxed(),
            PrimitiveArray::<T2>::from_vec(out_col2.drain(..).collect()).boxed(),
        ];
        let arr_order = order.drain(..).collect();
        res.push(MergedArrowChunk::new(out, arr_order));
    }

    Ok(res)
}


pub fn merge(chunks: Vec<ArrowChunk>, array_size: usize) -> Result<Vec<MergedArrowChunk>> {
    match chunks[0].arrs.len() {
        2 => {
            match (chunks[0].arrs[0].data_type().to_physical_type(), chunks[0].arrs[1].data_type().to_physical_type()) {
                (arrow2::datatypes::PhysicalType::Primitive(a), arrow2::datatypes::PhysicalType::Primitive(b)) => {
                    match (a, b) {
                        (PrimitiveType::Int64, PrimitiveType::Int64) => merge_two_primitives::<i64, i64>(chunks, array_size),
                        _ => unimplemented!("merge not implemented for {:?} {:?} primitive types", a, b)
                    }
                }
                _ => unimplemented!("merge not implemented for {:?} {:?} types", chunks[0].arrs[0].data_type(), chunks[0].arrs[1].data_type())
            }
        }
        _ => unimplemented!("merge not implemented for {:?} columns", chunks[0].arrs.len())
    }
}

#[cfg(test)]
mod tests {
    use arrow2::array::{Array, Int64Array, PrimitiveArray};
    use arrow2::compute::concatenate::concatenate;
    use arrow2::datatypes::DataType;
    use arrow2::io::parquet::read::deserialize::page_iter_to_arrays;
    use arrow2::io::parquet::write::{array_to_page_simple, WriteOptions};
    use parquet2::compression::CompressionOptions;
    use parquet2::encoding::Encoding;
    use parquet2::page::{CompressedPage, Page};
    use parquet2::read::decompress;
    use parquet2::schema::types::{PhysicalType, PrimitiveType};
    use parquet2::write::Version;
    use crate::parquet_new::arrow::{ArrowChunk, merge, merge_two_primitives};
    use crate::parquet_new::from_physical_type;
    use crate::parquet::parquet::CompressedDataPagesRow;


    #[test]
    fn test_merge_two_vec() {
        let row1 = {
            let arr1: Box<dyn Array> = Box::new(PrimitiveArray::<i64>::from_vec(vec![1, 1, 2, 2, 2]));
            let arr2: Box<dyn Array> = Box::new(PrimitiveArray::<i64>::from_vec(vec![1, 2, 1, 2, 3]));
            ArrowChunk::new(vec![arr1, arr2], 0)
        };
        let row2 = {
            let arr1: Box<dyn Array> = Box::new(PrimitiveArray::<i64>::from_vec(vec![3, 3, 4, 4, 4]));
            let arr2: Box<dyn Array> = Box::new(PrimitiveArray::<i64>::from_vec(vec![1, 2, 1, 2, 3]));
            ArrowChunk::new(vec![arr1, arr2], 0)
        };
        let row3 = {
            let arr1: Box<dyn Array> = Box::new(PrimitiveArray::<i64>::from_vec(vec![1, 1, 2, 2, 2]));
            let arr2: Box<dyn Array> = Box::new(PrimitiveArray::<i64>::from_vec(vec![1, 2, 1, 2, 3]));
            ArrowChunk::new(vec![arr1, arr2], 7)
        };
        let row4 = {
            let arr1: Box<dyn Array> = Box::new(PrimitiveArray::<i64>::from_vec(vec![1, 1, 2, 2, 2]));
            let arr2: Box<dyn Array> = Box::new(PrimitiveArray::<i64>::from_vec(vec![1, 2, 1, 2, 3]));
            ArrowChunk::new(vec![arr1, arr2], 9)
        };
        let row5 = {
            let arr1: Box<dyn Array> = Box::new(PrimitiveArray::<i64>::from_vec(vec![4]));
            let arr2: Box<dyn Array> = Box::new(PrimitiveArray::<i64>::from_vec(vec![4]));
            ArrowChunk::new(vec![arr1, arr2], 9)
        };

        let arr1_exp = Box::new(PrimitiveArray::<i64>::from_vec(vec![1]));
        let res = merge(vec![row1, row2, row3, row4, row5]).unwrap();

        println!("{:?}", res);
    }
}