use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fmt::Debug;

use arrow2::array::Array;
use arrow2::array::PrimitiveArray;
use arrow2::datatypes::Field;
use arrow2::datatypes::Schema;
use arrow2::types::NativeType;
use arrow2::types::PrimitiveType;

use crate::error::Result;
use crate::error::StoreError;

#[derive(Debug)]
// Arrow chunk after being merged
pub struct MergedArrowChunk {
    // cultiple arrays as a result of merging multiple chunks
    pub arrs: Vec<Box<dyn Array>>,
    // contains the stream id of each row so we can take the rows in the correct order
    pub reorder: Vec<usize>,
}

impl MergedArrowChunk {
    // Create new merged arrow chunk
    pub fn new(arrs: Vec<Box<dyn Array>>, reorder: Vec<usize>) -> Self {
        Self { arrs, reorder }
    }
}

#[derive(Debug)]
// Arrow chunk before being merged
// Arrow chunk is an unpacked CompressedPage
pub struct ArrowChunk {
    // stream of arrow chunk Used to identify the chunk during merge
    pub stream: usize,
    pub arrs: Vec<Box<dyn Array>>,
}

impl ArrowChunk {
    // Creates ArrowChunk
    pub fn new(arrs: Vec<Box<dyn Array>>, stream: usize) -> Self {
        Self { stream, arrs }
    }

    // Get length of arrow chunk
    pub fn len(&self) -> usize {
        self.arrs[0].len()
    }

    // Check if chunk is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

// Row for merge chunks with one column partition

// usize - stream id
// A - partition type
pub struct OneColMergeRow<A>(usize, A);

impl<A> Ord for OneColMergeRow<A>
where A: Ord
{
    fn cmp(&self, other: &Self) -> Ordering {
        self.1.cmp(&other.1).reverse()
    }
}

impl<A> PartialOrd for OneColMergeRow<A>
where A: Ord
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<A> PartialEq for OneColMergeRow<A>
where A: Eq
{
    fn eq(&self, other: &Self) -> bool {
        self.1 == other.1
    }
}

impl<A> Eq for OneColMergeRow<A> where A: Eq {}

// Row for merge chunks with two columns partition

// usize - stream id
// A,B - partition types
pub struct TwoColMergeRow<A, B>(usize, A, B);

impl<A, B> Ord for TwoColMergeRow<A, B>
where
    A: Ord,
    B: Ord,
{
    fn cmp(&self, other: &Self) -> Ordering {
        (&self.1, &self.2).cmp(&(&other.1, &other.2)).reverse()
    }
}

impl<A, B> PartialOrd for TwoColMergeRow<A, B>
where
    A: Ord,
    B: Ord,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<A, B> PartialEq for TwoColMergeRow<A, B>
where
    A: Eq,
    B: Eq,
{
    fn eq(&self, other: &Self) -> bool {
        (&self.1, &self.2) == (&other.1, &other.2)
    }
}

impl<A, B> Eq for TwoColMergeRow<A, B>
where
    A: Eq,
    B: Eq,
{
}

// merge chunks with single column partition which is primitive

// array_size - size of output arrays
pub fn merge_one_primitive<T: NativeType + Ord>(
    chunks: Vec<ArrowChunk>,
    array_size: usize,
) -> Result<Vec<MergedArrowChunk>> {
    // downcast to primitive array
    let mut arr_iters = chunks
        .iter()
        .map(|row| {
            row.arrs[0]
                .as_any()
                .downcast_ref::<PrimitiveArray<T>>()
                .unwrap()
                .values_iter()
        })
        .collect::<Vec<_>>();

    // use binary heap for sorting
    let mut sort = BinaryHeap::<OneColMergeRow<T>>::with_capacity(array_size);

    let mut res = vec![];
    // create buffers
    // perf: use predefined reusable buffers
    let mut out_col = Vec::with_capacity(array_size);
    let mut order = Vec::with_capacity(array_size);

    // push all the values in sorter
    for (row_id, iters) in arr_iters.iter_mut().enumerate().take(chunks.len()) {
        let mr = OneColMergeRow(row_id, *iters.next().unwrap());
        sort.push(mr);
    }

    // get sorted values
    while let Some(OneColMergeRow(row_idx, v)) = sort.pop() {
        out_col.push(v);
        order.push(chunks[row_idx].stream);
        if let Some(v) = arr_iters[row_idx].next() {
            let mr = OneColMergeRow(row_idx, *v);
            sort.push(mr);
        }

        // limit output by array size
        if out_col.len() >= array_size {
            let out = vec![PrimitiveArray::<T>::from_vec(out_col.drain(..).collect()).boxed()];
            let arr_order = order.drain(..).collect();
            res.push(MergedArrowChunk::new(out, arr_order));
        }
    }

    // drain the rest
    if !out_col.is_empty() {
        let out = vec![PrimitiveArray::<T>::from_vec(out_col.drain(..).collect()).boxed()];
        let arr_order = order.drain(..).collect();
        res.push(MergedArrowChunk::new(out, arr_order));
    }

    Ok(res)
}

// merge chunks with two column partition that are primitives

// array_size - size of output arrays
pub fn merge_two_primitives<T1: NativeType + Ord, T2: NativeType + Ord>(
    chunks: Vec<ArrowChunk>,
    array_size: usize,
) -> Result<Vec<MergedArrowChunk>> {
    let mut arr_iters = chunks
        .iter()
        .map(|row| {
            let arr1 = row.arrs[0]
                .as_any()
                .downcast_ref::<PrimitiveArray<T1>>()
                .unwrap()
                .values_iter();
            let arr2 = row.arrs[1]
                .as_any()
                .downcast_ref::<PrimitiveArray<T2>>()
                .unwrap()
                .values_iter();
            (arr1, arr2)
        })
        .collect::<Vec<_>>();

    let mut sort = BinaryHeap::<TwoColMergeRow<T1, T2>>::with_capacity(array_size);

    let mut res = vec![];
    let mut out_col1 = Vec::with_capacity(array_size);
    let mut out_col2 = Vec::with_capacity(array_size);
    let mut order = Vec::with_capacity(array_size);

    for (row_id, iter) in arr_iters.iter_mut().enumerate().take(chunks.len()) {
        let mr = TwoColMergeRow(row_id, *iter.0.next().unwrap(), *iter.1.next().unwrap());
        sort.push(mr);
    }

    while let Some(TwoColMergeRow(row_idx, v1, v2)) = sort.pop() {
        out_col1.push(v1);
        out_col2.push(v2);
        order.push(chunks[row_idx].stream);
        if let Some(v1) = arr_iters[row_idx].0.next() {
            let v2 = arr_iters[row_idx].1.next().unwrap();
            let mr = TwoColMergeRow(row_idx, *v1, *v2);
            sort.push(mr);
        }

        if out_col1.len() >= array_size {
            let out = vec![
                PrimitiveArray::<T1>::from_vec(out_col1.drain(..).collect()).boxed(),
                PrimitiveArray::<T2>::from_vec(out_col2.drain(..).collect()).boxed(),
            ];
            let arr_order = order.drain(..).collect();
            res.push(MergedArrowChunk::new(out, arr_order));
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

// Merge chunks

// Merge multiple chunks into vector of MergedArrowChunk of arrays split by array_size

// panics if merges is not implemented for combination of types
pub fn merge_chunks(chunks: Vec<ArrowChunk>, array_size: usize) -> Result<Vec<MergedArrowChunk>> {
    // supported lengths (count of index columns)
    match chunks[0].arrs.len() {
        1 => match chunks[0].arrs[0].data_type().to_physical_type() {
            arrow2::datatypes::PhysicalType::Primitive(pt) => match pt {
                PrimitiveType::Int64 => merge_one_primitive::<i64>(chunks, array_size),
                _ => unimplemented!("merge is not implemented for {pt:?} primitive type"),
            },
            _ => unimplemented!(
                "merge not implemented for {:?} type",
                chunks[0].arrs[0].data_type()
            ),
        },
        2 => {
            match (
                chunks[0].arrs[0].data_type().to_physical_type(),
                chunks[0].arrs[1].data_type().to_physical_type(),
            ) {
                (
                    arrow2::datatypes::PhysicalType::Primitive(a),
                    arrow2::datatypes::PhysicalType::Primitive(b),
                ) => {
                    match (a, b) {
                        // Put here possible combination that you need
                        // Or find a way to merge any types dynamically without performance penalty
                        (PrimitiveType::Int64, PrimitiveType::Int64) => {
                            merge_two_primitives::<i64, i64>(chunks, array_size)
                        }
                        (PrimitiveType::Int64, PrimitiveType::Int32) => {
                            merge_two_primitives::<i64, i32>(chunks, array_size)
                        }
                        _ => unimplemented!(
                            "merge is not implemented for {a:?} {b:?} primitive types"
                        ),
                    }
                }
                _ => unimplemented!(
                    "merge not implemented for {:?} {:?} types",
                    chunks[0].arrs[0].data_type(),
                    chunks[0].arrs[1].data_type()
                ),
            }
        }
        _ => unimplemented!(
            "merge not implemented for {:?} columns",
            chunks[0].arrs.len()
        ),
    }
}

// Merge arrow2 schemas
pub fn try_merge_schemas(schemas: Vec<Schema>) -> Result<Schema> {
    let fields: Result<Vec<Field>> = schemas.into_iter().map(|schema| schema.fields).try_fold(
        Vec::<Field>::new(),
        |mut merged, unmerged| {
            for field in unmerged.into_iter() {
                let merged_field = merged
                    .iter_mut()
                    .find(|merged_field| merged_field.name == field.name);
                match merged_field {
                    None => merged.push(field.to_owned()),
                    Some(merged_field) => {
                        if *merged_field != field {
                            return Err(StoreError::InvalidParameter(format!(
                                "Fields are not equal: {:?} {:?}",
                                merged_field, field
                            )));
                        }
                    }
                }
            }

            Ok(merged)
        },
    );

    Ok(Schema::from(fields?))
}

#[cfg(test)]
mod tests {
    use arrow2::array::Array;
    use arrow2::array::PrimitiveArray;
    use arrow2::datatypes::DataType;
    use arrow2::datatypes::Field;
    use arrow2::datatypes::Schema;

    use crate::merge::arrow::merge_chunks;
    use crate::merge::arrow::try_merge_schemas;
    use crate::merge::arrow::ArrowChunk;

    #[test]
    fn test_merge_schemas() -> anyhow::Result<()> {
        let s1 = Schema::from(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]);

        let s2 = Schema::from(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]);

        let res = try_merge_schemas(vec![s1.clone(), s2])?;
        assert_eq!(s1, res);

        let s1 = Schema::from(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
            Field::new("c", DataType::Utf8, false),
        ]);

        let s2 = Schema::from(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]);

        let res = try_merge_schemas(vec![s1.clone(), s2])?;
        assert_eq!(s1, res);

        let s1 = Schema::from(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
            Field::new("c", DataType::Utf8, false),
        ]);

        let s2 = Schema::from(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("d", DataType::Int64, false),
        ]);

        let res = try_merge_schemas(vec![s1.clone(), s2])?;

        let exp = Schema::from(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
            Field::new("c", DataType::Utf8, false),
            Field::new("d", DataType::Int64, false),
        ]);

        assert_eq!(exp, res);

        let s1 = Schema::from(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]);

        let s2 = Schema::from(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int8, false),
        ]);

        let res = try_merge_schemas(vec![s1.clone(), s2]);
        assert!(res.is_err());
        Ok(())
    }

    #[test]
    fn test_merge_one_vec() {
        let row1 = {
            let arr1: Box<dyn Array> =
                Box::new(PrimitiveArray::<i64>::from_vec(vec![1, 1, 2, 2, 2]));
            ArrowChunk::new(vec![arr1], 0)
        };
        let row2 = {
            let arr1: Box<dyn Array> =
                Box::new(PrimitiveArray::<i64>::from_vec(vec![3, 3, 4, 4, 4]));
            ArrowChunk::new(vec![arr1], 0)
        };
        let row3 = {
            let arr1: Box<dyn Array> =
                Box::new(PrimitiveArray::<i64>::from_vec(vec![1, 1, 2, 2, 2]));
            ArrowChunk::new(vec![arr1], 7)
        };
        let row4 = {
            let arr1: Box<dyn Array> =
                Box::new(PrimitiveArray::<i64>::from_vec(vec![1, 1, 2, 2, 2]));
            ArrowChunk::new(vec![arr1], 9)
        };
        let row5 = {
            let arr1: Box<dyn Array> = Box::new(PrimitiveArray::<i64>::from_vec(vec![4]));
            ArrowChunk::new(vec![arr1], 9)
        };

        let arr1_exp = Box::new(PrimitiveArray::<i64>::from_vec(vec![
            1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 3, 3, 4, 4, 4, 4,
        ]));
        let res = merge_chunks(vec![row1, row2, row3, row4, row5], 30).unwrap();

        assert_eq!(res[0].arrs[0], arr1_exp.boxed());
    }

    #[test]
    fn test_merge_two_vec() {
        let row1 = {
            let arr1: Box<dyn Array> =
                Box::new(PrimitiveArray::<i64>::from_vec(vec![1, 1, 2, 2, 2]));
            let arr2: Box<dyn Array> =
                Box::new(PrimitiveArray::<i64>::from_vec(vec![1, 2, 1, 2, 3]));
            ArrowChunk::new(vec![arr1, arr2], 0)
        };
        let row2 = {
            let arr1: Box<dyn Array> =
                Box::new(PrimitiveArray::<i64>::from_vec(vec![3, 3, 4, 4, 4]));
            let arr2: Box<dyn Array> =
                Box::new(PrimitiveArray::<i64>::from_vec(vec![1, 2, 1, 2, 3]));
            ArrowChunk::new(vec![arr1, arr2], 0)
        };
        let row3 = {
            let arr1: Box<dyn Array> =
                Box::new(PrimitiveArray::<i64>::from_vec(vec![1, 1, 2, 2, 2]));
            let arr2: Box<dyn Array> =
                Box::new(PrimitiveArray::<i64>::from_vec(vec![1, 2, 1, 2, 3]));
            ArrowChunk::new(vec![arr1, arr2], 7)
        };
        let row4 = {
            let arr1: Box<dyn Array> =
                Box::new(PrimitiveArray::<i64>::from_vec(vec![1, 1, 2, 2, 2]));
            let arr2: Box<dyn Array> =
                Box::new(PrimitiveArray::<i64>::from_vec(vec![1, 2, 1, 2, 3]));
            ArrowChunk::new(vec![arr1, arr2], 9)
        };
        let row5 = {
            let arr1: Box<dyn Array> = Box::new(PrimitiveArray::<i64>::from_vec(vec![4]));
            let arr2: Box<dyn Array> = Box::new(PrimitiveArray::<i64>::from_vec(vec![4]));
            ArrowChunk::new(vec![arr1, arr2], 9)
        };

        let _arr1_exp = Box::new(PrimitiveArray::<i64>::from_vec(vec![1]));
        let res = merge_chunks(vec![row1, row2, row3, row4, row5], 12).unwrap();

        println!("{:?}", res);
    }
}
