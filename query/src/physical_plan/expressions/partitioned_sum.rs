use std::convert::TryFrom;
use std::fmt::Debug;
use std::ops::Add;
use std::sync::{Arc, Mutex, RwLock};

use crate::error::{Error, Result};
use arrow::array::{
    Array, ArrayBuilder, ArrayRef, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    Int8Array, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::compute;
use arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion::physical_plan::{expressions, Accumulator, AggregateExpr, PhysicalExpr};
use datafusion::physical_plan::aggregates::return_type;
use datafusion::physical_plan::expressions::{Avg, AvgAccumulator, Count, Literal, Max, Min, Sum};
use datafusion::scalar::ScalarValue;
use crate::physical_plan::expressions::aggregate::{AggregateFunction};
use crate::physical_plan::expressions::partitioned_aggregate::{Buffer, PartitionedAccumulator, PartitionedAggregate, Value};
use dyn_clone::DynClone;

#[derive(Debug)]
pub struct PartitionedSumAccumulator {
    sum: Value,
    buffer: Buffer,
}

const CAP: usize = 1000;

impl PartitionedSumAccumulator {
    pub fn try_new(data_type: DataType, outer_acc: Box<dyn Accumulator>) -> Result<Self> {
        let value = match data_type {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => Value::Int64(0),
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => Value::UInt64(0),
            DataType::Float16 | DataType::Float32 | DataType::Float64 => Value::Float64(0.0),
            _ => unimplemented!(),
        };
        Ok(Self {
            sum: value,
            buffer: Buffer::new(CAP, data_type.clone(), outer_acc),
        })
    }
}

macro_rules! update_batch {
    ($self:ident, $array:expr, $spans:expr, $type:ident, $vtype:ident, $ARRAYTYPE:ident) => {{
        let mut sum: $type = $self.sum.into();
        let arr = $array.as_any().downcast_ref::<$ARRAYTYPE>().unwrap();
        for (idx, value) in arr.iter().enumerate() {
            if $spans[idx] {
                $self.buffer.push(Value::$vtype(sum));
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

impl PartitionedAccumulator for PartitionedSumAccumulator {
    fn update_batch(&mut self, spans: &[bool], values: &[ArrayRef]) -> Result<()> {
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

        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        Ok(self.buffer.merge_batch(states)?)
    }

    fn state(&self) -> Result<Vec<ScalarValue>> {
        self.buffer.flush_with_value(self.sum.clone())?;
        Ok(self.buffer.state()?)
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        self.buffer.flush_with_value(self.sum.clone())?;
        Ok(self.buffer.evaluate()?)
    }
}

#[cfg(test)]
mod tests {
    use crate::error::Result;
    use arrow::array::{ArrayRef, Int8Array};
    use arrow::datatypes::DataType;
    use datafusion::scalar::ScalarValue as DFScalarValue;
    use std::sync::Arc;
    use datafusion::physical_plan::AggregateExpr;
    use datafusion::physical_plan::expressions::{Avg, AvgAccumulator, Literal};
    use datafusion_common::ScalarValue;
    use datafusion_expr::Accumulator;
    use crate::physical_plan::expressions::aggregate::{AggregateFunction};
    use crate::physical_plan::expressions::partitioned_aggregate::PartitionedAccumulator;
    use crate::physical_plan::expressions::partitioned_sum::PartitionedSumAccumulator;

    #[test]
    fn test() -> Result<()> {
        let outer_acc: Box<dyn Accumulator> = Box::new(AvgAccumulator::try_new(&DataType::Float64)?);
        let mut sum_acc = PartitionedSumAccumulator::try_new(DataType::Int64, outer_acc)?;
        let spans = vec![
            false, false, true, false, false, true, false, false, false, true,
        ];
        //                             v        v           v
        let vals = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let arr = Arc::new(Int8Array::from(vals));

        sum_acc.update_batch(&spans, &[arr.clone() as ArrayRef]);
        sum_acc.update_batch(&spans, &[arr.clone() as ArrayRef]);

        let list = vec![1 + 2, 3 + 4 + 5, 6 + 7 + 8 + 9, 10 + 1 + 2, 3 + 4 + 5, 6 + 7 + 8 + 9, 10];
        let sum: i32 = Iterator::sum(list.iter());
        let mean = sum as f64 / (list.len() as f64);

        assert_eq!(sum_acc.evaluate()?, DFScalarValue::Float64(Some(mean)));
        Ok(())
    }
}
