use std::convert::TryFrom;
use std::ops::Add;

use crate::error::{Error, Result};
use arrow::array::{
    Array, ArrayBuilder, ArrayRef, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    Int8Array, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::compute;
use arrow::datatypes::DataType;
use datafusion::scalar::ScalarValue;

#[derive(Debug, Clone)]
pub struct SumAccumulator<V> {
    sum: V,
    result_buffer: Vec<V>,
}

const CAP: usize = 1000;

impl<V> SumAccumulator<V>
where
    V: Default,
{
    /// new sum accumulator
    pub fn new(v: V) -> Result<Self> {
        Ok(Self {
            sum: v,
            result_buffer: Vec::with_capacity(CAP),
        })
    }
}

pub trait Aggregator<T> {
    fn unit() -> T;
    fn accumulate(accumulator: T, value: i64) -> T;
    fn combine(accumulator1: i64, accumulator2: i64) -> i64;
}

impl<V> SumAccumulator<V>
where
    V: Default + Clone + Add,
{
    fn update_batch(&mut self, spans: &[bool], values: &[ArrayRef]) -> Result<()> {
        let val_arr = values[0].as_ref();
        match val_arr.data_type() {
            DataType::Int8 => {
                let mut sum = self.sum.clone();
                let arr = val_arr.as_any().downcast_ref::<Int8Array>()?;
                for (idx, value) in arr.iter().enumerate() {
                    if spans[idx] {
                        self.result_buffer.push(sum.clone());
                        self.sum = V::default();
                    }

                    match value {
                        None => continue,
                        Some(value) => {
                            sum = sum + value;
                        }
                    }
                }
                self.sum = sum;
            }
            _ => unimplemented!(),
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::error::{Error, Result};
    use crate::physical_plan::expressions::partitioned_sum::SumAccumulator;
    use arrow::array::Int8Array;

    #[test]
    fn test() -> Result<()> {
        let mut acc = SumAccumulator::new(0i64)?;
        let spans = vec![
            false, false, true, false, false, true, false, false, false, true,
        ];
        let arr = Int8Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        acc.update_batch(&spans, &[arr]);
        Ok(())
    }
}
