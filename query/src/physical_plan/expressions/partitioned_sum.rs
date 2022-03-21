use std::convert::TryFrom;
use std::fmt::Debug;
use std::ops::Add;
use std::sync::{Arc, RwLock};

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
use crate::physical_plan::expressions::partitioned_aggregate::{PartitionedAccumulator, PartitionedAggregate, Value};
use dyn_clone::DynClone;

#[derive(Debug)]
pub struct PartitionedSumAccumulator {
    sum: Value,
    data_type: DataType,
    result_buffer: Vec<Value>,
    outer_acc: Box<dyn Accumulator>,
    outer_agg: AggregateFunction,
}

const CAP: usize = 1000;

impl PartitionedSumAccumulator {
    pub fn try_new(data_type: DataType, outer_acc: Box<dyn Accumulator>, outer_agg: AggregateFunction) -> Result<Self> {
        let value = match data_type {
            DataType::Int64 => Value::Int64(0),
            _ => unimplemented!(),
        };
        Ok(Self {
            sum: value,
            data_type,
            result_buffer: Vec::with_capacity(CAP),
            outer_acc,
            outer_agg,
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
        $self.outer_acc.update_batch(&[Arc::new(arr) as ArrayRef])?;
        $self
            .result_buffer
            .resize(0, Value::$vtype($type::default()));
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

        if self.result_buffer.len() > CAP {
            self.flush_buffer();
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        Ok(self.outer_acc.merge_batch(states)?)
    }

    fn state(&self) -> Result<Vec<ScalarValue>> {
        /*if !self.result_buffer.is_empty() {
            self.flush_buffer();
        }*/
        Ok(self.outer_acc.state()?)
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        if !self.result_buffer.is_empty() {
            let mut outer_acc: Box<dyn Accumulator> = Box::new(match self.outer_agg {
                AggregateFunction::Avg => AvgAccumulator::try_new(&self.outer_acc.evaluate()?.get_datatype())?,
                _ => unimplemented!(),
            });

            let outer_state: Vec<ArrayRef> = self.outer_acc.state()?.iter().map(|v| v.to_array()).collect();
            outer_acc.merge_batch(&outer_state)?;
            let mut acc = PartitionedSumAccumulator {
                sum: self.sum.clone(),
                data_type: self.data_type.clone(),
                result_buffer: self.result_buffer.clone(),
                outer_acc,
                outer_agg: self.outer_agg.clone(),
            };

            acc.flush_buffer();
            return Ok(acc.outer_acc.evaluate()?);
        }

        Ok(self.outer_acc.evaluate()?)
    }
}

impl PartitionedSumAccumulator {
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
    use arrow::array::{ArrayRef, Int8Array};
    use arrow::datatypes::DataType;
    use datafusion::scalar::ScalarValue as DFScalarValue;
    use std::sync::Arc;
    use datafusion::physical_plan::AggregateExpr;
    use datafusion::physical_plan::expressions::{Avg, Literal};
    use datafusion_common::ScalarValue;
    use crate::physical_plan::expressions::aggregate::{AggregateFunction};
    use crate::physical_plan::expressions::partitioned_aggregate::PartitionedAccumulator;
    use crate::physical_plan::expressions::partitioned_sum::PartitionedSumAccumulator;

    #[test]
    fn test() -> Result<()> {
        let expr = Avg::new(Arc::new(Literal::new(ScalarValue::from(true))), "avg", DataType::Float64);
        let outer_acc = expr.create_accumulator()?;
        let mut sum_acc = PartitionedSumAccumulator::try_new(DataType::Int64, outer_acc, AggregateFunction::Avg)?;
        //                                        3                   12                         30
        let spans = vec![
            false, false, true, false, false, true, false, false, false, true,
        ];
        let arr = Arc::new(Int8Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]));
        sum_acc.update_batch(&spans, &[arr.clone() as ArrayRef]);
        sum_acc.update_batch(&spans, &[arr.clone() as ArrayRef]);
        assert_eq!(sum_acc.evaluate()?, DFScalarValue::Int64(Some(100)));
        Ok(())
    }
}
