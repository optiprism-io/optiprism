use std::convert::TryFrom;
use std::ops::Add;
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::tt::sum;
use arrow::array::{
    Array, ArrayBuilder, ArrayRef, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    Int8Array, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::compute;
use arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion::physical_plan::{expressions, Accumulator};
use datafusion::scalar::ScalarValue;

// enum storage for accumulator for static dispatching
#[derive(Debug, Clone)]
pub enum AccumulatorEnum {
    Sum(sum::SumAccumulator),
}

impl Accumulator for AccumulatorEnum {
    fn state(&self) -> DFResult<Vec<ScalarValue>> {
        match self {
            AccumulatorEnum::Sum(acc) => acc.state(),
        }
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        match self {
            AccumulatorEnum::Sum(acc) => acc.update_batch(values),
        }
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        match self {
            AccumulatorEnum::Sum(acc) => acc.merge_batch(states),
        }
    }

    fn evaluate(&self) -> DFResult<ScalarValue> {
        match self {
            AccumulatorEnum::Sum(acc) => acc.evaluate(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum Value {
    Int64(i64),
    UInt64(u64),
    Float64(f64),
}

impl From<Value> for i64 {
    fn from(v: Value) -> Self {
        match v {
            Value::Int64(v) => v,
            _ => unreachable!(),
        }
    }
}

impl From<&Value> for i64 {
    fn from(v: &Value) -> Self {
        match v {
            Value::Int64(v) => *v,
            _ => unreachable!(),
        }
    }
}

impl From<Value> for u64 {
    fn from(v: Value) -> Self {
        match v {
            Value::UInt64(v) => v,
            _ => unreachable!(),
        }
    }
}

impl From<&Value> for u64 {
    fn from(v: &Value) -> Self {
        match v {
            Value::UInt64(v) => *v,
            _ => unreachable!(),
        }
    }
}

impl From<Value> for f64 {
    fn from(v: Value) -> Self {
        match v {
            Value::Float64(v) => v,
            _ => unreachable!(),
        }
    }
}

impl From<&Value> for f64 {
    fn from(v: &Value) -> Self {
        match v {
            Value::Float64(v) => *v,
            _ => unreachable!(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SumAccumulator {
    sum: Value,
    result_buffer: Vec<Value>,
    acc: AccumulatorEnum,
}

const CAP: usize = 1000;

impl SumAccumulator {
    /// new sum accumulator
    pub fn try_new(data_type: &DataType, acc: AccumulatorEnum) -> Result<Self> {
        let value = match data_type {
            DataType::Int64 => Value::Int64(0),
            _ => unimplemented!(),
        };
        Ok(Self {
            sum: value,
            result_buffer: Vec::with_capacity(CAP),
            acc,
        })
    }
}

macro_rules! distinct_count_array {
    ($array:expr, $ARRAYTYPE:ident, $state:expr) => {{
        let array_size = $array.len();
        let typed_array = $array.as_any().downcast_ref::<$ARRAYTYPE>().unwrap();
        for index in 0..array_size {
            if !$state.current.eq_array($array, index) {
                $state.current = typed_array.value(index).into();
                $state.count += 1;
            }
        }
        Ok(())
    }};
}

macro_rules! update_batch {
    ($self:ident, $array:expr, $spans:expr, $type:ident, $vtype:ident, $ARRAYTYPE:ident) => {{
        let mut sum: $type = $self.sum.into();
        let arr = $array.as_any().downcast_ref::<$ARRAYTYPE>().unwrap();
        for (idx, value) in arr.iter().enumerate() {
            if $spans[idx] {
                $self.result_buffer.push(Value::$vtype(sum));
                sum = $type::default();
            }

            match value {
                None => continue,
                Some(value) => {
                    sum += value as $type;
                }
            }
        }
        $self.sum = Value::$vtype(sum)
    }};
}

macro_rules! flush_buffer {
    ($self:ident, $type:ident, $vtype:ident, $ARRAYTYPE:ident) => {{
        let arr = $ARRAYTYPE::from(
            $self
                .result_buffer
                .iter()
                .map(|v| v.into())
                .collect::<Vec<$type>>(),
        );
        $self.acc.update_batch(&[Arc::new(arr) as ArrayRef])?;
        $self
            .result_buffer
            .resize(0, Value::$vtype($type::default()));
    }};
}

impl SumAccumulator {
    pub fn update_batch(&mut self, spans: &[bool], values: &[ArrayRef]) -> Result<()> {
        let val_arr = values[0].as_ref();
        match val_arr.data_type() {
            DataType::UInt8 => update_batch!(self, val_arr, spans, u64, UInt64, UInt8Array),
            DataType::UInt16 => update_batch!(self, val_arr, spans, u64, UInt64, UInt16Array),
            DataType::UInt32 => update_batch!(self, val_arr, spans, u64, UInt64, UInt32Array),
            DataType::UInt64 => update_batch!(self, val_arr, spans, u64, UInt64, UInt64Array),
            DataType::Int8 => update_batch!(self, val_arr, spans, i64, Int64, Int8Array),
            DataType::Int16 => update_batch!(self, val_arr, spans, i64, Int64, Int16Array),
            DataType::Int32 => update_batch!(self, val_arr, spans, i64, Int64, Int32Array),
            DataType::Int64 => update_batch!(self, val_arr, spans, i64, Int64, Int64Array),
            DataType::Float32 => update_batch!(self, val_arr, spans, f64, Float64, Float32Array),
            DataType::Float64 => update_batch!(self, val_arr, spans, f64, Float64, Float64Array),
            _ => unimplemented!(),
        }

        if self.result_buffer.len() > CAP {
            self.flush_buffer();
        }
        Ok(())
    }

    pub fn state(&mut self) -> Result<Vec<ScalarValue>> {
        if !self.result_buffer.is_empty() {
            self.flush_buffer();
        }

        Ok(self.acc.state()?)
    }

    fn flush_buffer(&mut self) -> Result<()> {
        match self.sum {
            Value::Int64(_) => flush_buffer!(self, i64, Int64, Int64Array),
            Value::UInt64(_) => flush_buffer!(self, u64, UInt64, UInt64Array),
            Value::Float64(_) => flush_buffer!(self, f64, Float64, Float64Array),
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::error::Result;
    use crate::tt::partitioned_sum::{AccumulatorEnum, SumAccumulator};
    use crate::tt::sum::SumAccumulator as SumAcc;
    use arrow::array::{ArrayRef, Int8Array};
    use arrow::datatypes::DataType;
    use datafusion::scalar::ScalarValue as DFScalarValue;
    use std::sync::Arc;

    #[test]
    fn test() -> Result<()> {
        let mut sum_acc = AccumulatorEnum::Sum(SumAcc::try_new(&DataType::Int64)?);
        let mut acc = SumAccumulator::try_new(&DataType::Int64, sum_acc)?;
        //                                        3                   12                         30
        let spans = vec![
            false, false, true, false, false, true, false, false, false, true,
        ];
        let arr = Arc::new(Int8Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]));
        acc.update_batch(&spans, &[arr.clone() as ArrayRef]);
        acc.update_batch(&spans, &[arr.clone() as ArrayRef]);
        assert_eq!(acc.state()?, vec![DFScalarValue::Int64(Some(100))]);
        Ok(())
    }
}
