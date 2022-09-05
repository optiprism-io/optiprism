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

use crate::error::{ QueryError, Result};
use std::cmp::Ordering;
use std::convert::TryFrom;
use std::fmt;
use std::fmt::Debug;

use std::sync::{Arc, Mutex};

use arrow::array::{Array, ArrayRef, Decimal128Builder, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, UInt16Array, UInt32Array, UInt64Array, UInt8Array};
use arrow::datatypes::DataType;
use datafusion_common::Result as DFResult;

use crate::physical_plan::expressions::partitioned_count::PartitionedCountAccumulator;
use crate::physical_plan::expressions::partitioned_sum::PartitionedSumAccumulator;

use datafusion::physical_plan::expressions::{AvgAccumulator, MaxAccumulator, MinAccumulator};
use datafusion::physical_plan::Accumulator;
use datafusion_common::ScalarValue;
use datafusion_expr::{AggregateFunction, AggregateState};

// PartitionedAccumulator extends Accumulator trait with reset
pub trait PartitionedAccumulator: Debug + Send + Sync {
    fn update_batch(&mut self, spans: &[bool], values: &[ArrayRef]) -> Result<()>;
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()>;
    fn state(&self) -> Result<Vec<AggregateState>>;
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
    ($buffer:ident, $type:ident, $ARRAYTYPE:ident) => {{
        Arc::new($ARRAYTYPE::from(
            $buffer.iter().map(|v| v.into()).collect::<Vec<$type>>(),
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
        self._flush(&buffer)
    }

    pub fn flush(&self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        self._flush(&self.buffer)
    }

    fn _flush(&self, buffer: &[Value]) -> Result<()> {
        let arr = match self.data_type {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                buffer_to_array_ref!(buffer, i64, Int64Array)
            }
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                buffer_to_array_ref!(buffer, u64, UInt64Array)
            }
            DataType::Float32 | DataType::Float64 => {
                buffer_to_array_ref!(buffer, f64, Float64Array)
            }
            DataType::Decimal128(precision, scale) => {
                let mut builder = Decimal128Builder::new(buffer.len(), precision, scale);
                for v in buffer.iter() {
                    builder.append_value(v)?;
                }
                Arc::new(builder.finish()) as ArrayRef
            }
            _ => unimplemented!("{:?}", self.data_type),
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

    pub fn state(&self) -> DFResult<Vec<AggregateState>> {
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
    Decimal128(i128),
    Null,
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

impl From<Value> for i128 {
    fn from(v: Value) -> Self {
        match v {
            Value::Decimal128(v) => v,
            _ => unreachable!(),
        }
    }
}

impl From<&Value> for i128 {
    fn from(v: &Value) -> Self {
        match v {
            Value::Decimal128(v) => *v,
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
    _outer_agg: AggregateFunction,
    data_type: DataType,
) -> Result<Box<dyn PartitionedAccumulator>> {
    Ok(match agg {
        PartitionedAggregateFunction::Sum => {
            Box::new(PartitionedSumAccumulator::try_new(data_type, outer_acc)?)
        }
        PartitionedAggregateFunction::Count => {
            Box::new(PartitionedCountAccumulator::try_new(outer_acc)?)
        }
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
        let outer_acc = match outer_agg {
            AggregateFunction::Avg => {
                Ok(Box::new(AvgAccumulator::try_new(agg_return_type)?) as Box<dyn Accumulator>)
            }
            AggregateFunction::Min => {
                Ok(Box::new(MinAccumulator::try_new(agg_return_type)?) as Box<dyn Accumulator>)
            }
            AggregateFunction::Max => {
                Ok(Box::new(MaxAccumulator::try_new(agg_return_type)?) as Box<dyn Accumulator>)
            }
            _ => Err(QueryError::Plan(format!(
                "outer aggregate function \"{:?}\" doesn't supported",
                outer_agg
            ))),
        }?;

        Ok(Self {
            last_partition_value: ScalarValue::try_from(partition_type)?,
            first_row: true,
            acc: new_partitioned_accumulator(agg, outer_acc, outer_agg.clone(), data_type.clone())?,
        })
    }
}

macro_rules! make_spans {
    ($self:ident, $values:expr,$type:ident, $scalar_type:ident, $ARRAYTYPE:ident) => {{
        let mut spans = Vec::with_capacity($values[0].len());

        let arr = $values[0].as_any().downcast_ref::<$ARRAYTYPE>().unwrap();
        let mut last_value: Option<$type> = if $self.first_row {
            $self.first_row = false;
            match arr.is_null(0) {
                true => None,
                false => Some(arr.value(0)),
            }
        } else {
            match &$self.last_partition_value {
                ScalarValue::$scalar_type(v) => *v,
                _ => unreachable!(),
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

        $self
            .acc
            .update_batch(&spans, &$values[1..])
            .map_err(QueryError::into_datafusion_execution_error)?
    }};
}
impl Accumulator for PartitionedAggregateAccumulator {
    fn state(&self) -> DFResult<Vec<AggregateState>> {
        self.acc
            .state()
            .map_err(QueryError::into_datafusion_execution_error)
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        match values[0].data_type() {
            DataType::Int8 => make_spans!(self, values, i8, Int8, Int8Array),
            DataType::Int16 => make_spans!(self, values, i16, Int16, Int16Array),
            DataType::Int32 => make_spans!(self, values, i32, Int32, Int32Array),
            DataType::Int64 => make_spans!(self, values, i64, Int64, Int64Array),
            DataType::UInt8 => make_spans!(self, values, u8, UInt8, UInt8Array),
            DataType::UInt16 => make_spans!(self, values, u16, UInt16, UInt16Array),
            DataType::UInt32 => make_spans!(self, values, u32, UInt32, UInt32Array),
            DataType::UInt64 => make_spans!(self, values, u64, UInt64, UInt64Array),
            _ => unimplemented!(),
        };
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        self.acc
            .merge_batch(states)
            .map_err(QueryError::into_datafusion_execution_error)
    }

    fn evaluate(&self) -> DFResult<ScalarValue> {
        self.acc
            .evaluate()
            .map_err(QueryError::into_datafusion_execution_error)
    }
}
