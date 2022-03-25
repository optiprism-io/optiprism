// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Defines physical expressions that can evaluated at runtime during query execution

use std::cmp::Ordering;
use std::convert::TryFrom;
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use crate::error::{Error, Result};

use arrow::array::{Array, ArrayRef, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, UInt16Array, UInt32Array, UInt64Array, UInt8Array};
use arrow::datatypes::DataType;
use dyn_clone::DynClone;
use datafusion::error::{DataFusionError, Result as DFResult};


use datafusion::physical_plan::{Accumulator, AggregateExpr};
use datafusion::physical_plan::aggregates::return_type;
use datafusion::physical_plan::expressions::{Avg, AvgAccumulator, Count, Literal, Max, Min, Sum};
use datafusion::scalar::ScalarValue;
use crate::physical_plan::expressions::aggregate::{AggregateFunction};
use crate::physical_plan::expressions::partitioned_count::PartitionedCountAccumulator;
use crate::physical_plan::expressions::partitioned_sum::{PartitionedSumAccumulator};

// PartitionedAccumulator extends Accumulator trait with reset
pub trait PartitionedAccumulator: Debug + Send + Sync {
    fn update_batch(&mut self, spans: &[bool], values: &[ArrayRef]) -> Result<()>;
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()>;
    fn state(&self) -> Result<Vec<ScalarValue>>;
    fn evaluate(&self) -> Result<ScalarValue>;
}

#[derive(Clone, Debug)]
pub struct Buffer {
    cap: usize,
    data_type: DataType,
    buffer: Vec<Value>,
    acc: Arc<Mutex<Box<dyn Accumulator>>>,
}

macro_rules! buffer_to_array_ref {
    ($buffer:ident, $type:ident, $vtype:ident, $ARRAYTYPE:ident) => {{
        Arc::new($ARRAYTYPE::from(
            $buffer
                .iter()
                .map(|v| v.into())
                .collect::<Vec<$type>>(),
        )) as ArrayRef
    }};
}

impl Buffer {
    pub fn new(cap: usize, data_type: DataType, acc: Box<dyn Accumulator>) -> Self {
        Self {
            cap,
            data_type,
            buffer: Vec::with_capacity(cap),
            acc: Arc::new(Mutex::new(acc)),
        }
    }

    pub fn push(&mut self, value: Value) -> Result<()> {
        self.buffer.push(value);

        if self.buffer.len() >= self.cap {
            self.flush()?;
            self.reset();
        }
        Ok(())
    }

    pub fn flush_with_value(&self, value: Value) -> Result<()> {
        let mut buffer = self.buffer.clone();
        buffer.push(value);
        let arr = match self.data_type {
            DataType::Int64 => buffer_to_array_ref!(buffer, i64, Int64, Int64Array),
            DataType::UInt64 => buffer_to_array_ref!(buffer, u64, UInt64, UInt64Array),
            DataType::Float64 => buffer_to_array_ref!(buffer, f64, Float64, Float64Array),
            _ => unimplemented!(),
        };

        let mut acc = self.acc.lock().unwrap();
        Ok(acc.update_batch(&[arr])?)
    }

    pub fn flush(&self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        let buf = &self.buffer;
        let arr = match self.data_type {
            DataType::Int64 => buffer_to_array_ref!(buf, i64, Int64, Int64Array),
            DataType::UInt64 => buffer_to_array_ref!(buf, u64, UInt64, UInt64Array),
            DataType::Float64 => buffer_to_array_ref!(buf, f64, Float64, Float64Array),
            _ => unimplemented!(),
        };

        let mut acc = self.acc.lock().unwrap();
        Ok(acc.update_batch(&[arr])?)
    }

    pub fn reset(&mut self) {
        self.buffer.resize(0, Value::Null);
    }

    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    pub fn state(&self) -> DFResult<Vec<ScalarValue>> {
        self.acc.lock().unwrap().state()
    }

    pub fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        self.acc.lock().unwrap().update_batch(values)
    }

    pub fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        self.acc.lock().unwrap().merge_batch(states)
    }

    pub fn evaluate(&self) -> DFResult<ScalarValue> {
        self.acc.lock().unwrap().evaluate()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum PartitionedAggregateFunction {
    Count,
    Sum,
}

impl fmt::Display for PartitionedAggregateFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", format!("{:?}", self).to_uppercase())
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Value {
    Int64(i64),
    UInt64(u64),
    Float64(f64),
    Null,
}

impl Value {
    pub fn get_datatype(&self) -> DataType {
        match self {
            Value::Int64(_) => DataType::Int64,
            Value::UInt64(_) => DataType::UInt64,
            Value::Float64(_) => DataType::Float64,
            _ => unreachable!()
        }
    }
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

impl From<u64> for Value {
    fn from(v: u64) -> Self {
        Value::UInt64(v)
    }
}

const MAX_BUFFER_SIZE: usize = 1000;

// partitioned aggregate is used as a accumulator factory from closure
pub struct PartitionedAggregate {
    partition_type: DataType,
    data_type: DataType,
    agg: PartitionedAggregateFunction,
    agg_return_type: DataType,
    outer_agg: AggregateFunction,
}

impl PartitionedAggregate {
    pub fn try_new(
        partition_type: DataType,
        data_type: DataType,
        agg: PartitionedAggregateFunction,
        agg_return_type: DataType,
        outer_agg: AggregateFunction,
    ) -> Result<Self> {
        Ok(Self {
            partition_type,
            data_type,
            agg,
            agg_return_type,
            outer_agg,
        })
    }

    pub fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(PartitionedAggregateAccumulator::try_new(
            &self.partition_type,
            &self.data_type,
            &self.agg,
            &self.agg_return_type,
            &self.outer_agg,
        )?))
    }
}

// partitioned aggregate accumulator aggregates incoming partitioned values via acc accumulator and
// aggregates acc result via outer_acc
#[derive(Debug)]
pub struct PartitionedAggregateAccumulator {
    last_partition_value: ScalarValue,
    first_row: bool,
    acc: Box<dyn PartitionedAccumulator>,
}

fn new_partitioned_accumulator(
    agg: &PartitionedAggregateFunction,
    outer_acc: Box<dyn Accumulator>,
    outer_agg: AggregateFunction,
    data_type: DataType,
) -> Result<Box<dyn PartitionedAccumulator>> {
    Ok(match agg {
        PartitionedAggregateFunction::Sum => Box::new(PartitionedSumAccumulator::try_new(data_type, outer_acc)?),
        PartitionedAggregateFunction::Count => Box::new(PartitionedCountAccumulator::try_new(outer_acc)?)
    })
}

pub fn state_types(data_type: DataType, agg: &AggregateFunction) -> Result<Vec<DataType>> {
    Ok(match agg {
        AggregateFunction::Count => vec![DataType::UInt64],
        AggregateFunction::Sum => vec![data_type],
        AggregateFunction::Avg => vec![DataType::UInt64, data_type],
        _ => unimplemented!(),
    })
}

impl PartitionedAggregateAccumulator {
    /// new sum accumulator
    pub fn try_new(
        partition_type: &DataType,
        data_type: &DataType,
        agg: &PartitionedAggregateFunction,
        agg_return_type: &DataType,
        outer_agg: &AggregateFunction,
    ) -> Result<Self> {
        let expr = Arc::new(Literal::new(ScalarValue::from(true)));
        let outer_acc: Box<dyn Accumulator> = Box::new(match outer_agg {
            AggregateFunction::Avg => Ok(AvgAccumulator::try_new(agg_return_type)?),
            _ => Err(Error::Internal(format!("{:?} doesn't supported", outer_agg))),
        }?);

        Ok(Self {
            last_partition_value: ScalarValue::try_from(partition_type)?,
            first_row: true,
            acc: new_partitioned_accumulator(agg, outer_acc, outer_agg.clone(), data_type.clone())?,
        })
    }
}

macro_rules! update_batch {
    ($self:ident, $values:expr,$type:ident, $scalar_type:ident, $ARRAYTYPE:ident)=> {{
        let mut spans = Vec::with_capacity($values[0].len());

        let arr = $values[0].as_any().downcast_ref::<$ARRAYTYPE>().unwrap();
        let mut last_value: Option<$type> = if $self.first_row {
            $self.first_row = false;
            match arr.is_null(0) {
                true => None,
                false => Some(arr.value(0))
            }
        } else {
            match &$self.last_partition_value {
                ScalarValue::$scalar_type(v) => *v,
                _ => unreachable!()
            }
        };

        for v in arr.iter() {
            match last_value.partial_cmp(&v) {
                None => unreachable!(),
                Some(ord) => match ord {
                    Ordering::Less | Ordering::Greater => {
                        spans.push(true);
                        last_value = v.clone();
                    }
                    Ordering::Equal => spans.push(false),
                },
            };
        }

        $self.last_partition_value = ScalarValue::$scalar_type(last_value);

        $self.acc.update_batch(&spans, &$values[1..]).map_err(Error::into_datafusion_execution_error);
}}
}
impl Accumulator for PartitionedAggregateAccumulator {
    fn state(&self) -> DFResult<Vec<ScalarValue>> {
        self.acc.state().map_err(Error::into_datafusion_execution_error)
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        match values[0].data_type() {
            DataType::Int8 => update_batch!(self,values,i8,Int8,Int8Array),
            DataType::Int16 => update_batch!(self,values,i16,Int16,Int16Array),
            DataType::Int32 => update_batch!(self,values,i32,Int32,Int32Array),
            DataType::Int64 => update_batch!(self,values,i64,Int64,Int64Array),
            DataType::UInt8 => update_batch!(self,values,u8,UInt8,UInt8Array),
            DataType::UInt16 => update_batch!(self,values,u16,UInt16,UInt16Array),
            DataType::UInt32 => update_batch!(self,values,u32,UInt32,UInt32Array),
            DataType::UInt64 => update_batch!(self,values,u64,UInt64,UInt64Array),
            _ => unimplemented!()
        };
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        self.acc
            .merge_batch(states)
            .map_err(Error::into_datafusion_execution_error)
    }

    fn evaluate(&self) -> DFResult<ScalarValue> {
        self.acc
            .evaluate()
            .map_err(Error::into_datafusion_execution_error)
    }
}
