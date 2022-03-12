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
use std::fmt::Debug;
use crate::error::{Error, Result};

use arrow::array::{Array, ArrayRef, Int8Array};
use arrow::datatypes::DataType;
use datafusion::error::{DataFusionError, Result as DFResult};


use datafusion::physical_plan::Accumulator;
use datafusion::physical_plan::aggregates::return_type;
use datafusion::scalar::ScalarValue;
use crate::physical_plan::expressions::aggregate::{AccumulatorEnum, AggregateFunction, new_accumulator};
use crate::physical_plan::expressions::partitioned_sum::PartitionedSumAccumulator;

// PartitionedAccumulator extends Accumulator trait with reset
pub trait PartitionedAccumulator: Debug + Send + Sync {
    fn update_batch(&mut self, spans: &[bool], values: &[ArrayRef]) -> Result<()>;
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()>;
    fn state(&mut self) -> Result<Vec<ScalarValue>>;
    fn evaluate(&self) -> Result<ScalarValue>;
    fn clone(&self) -> Self where Self: Sized;
}


#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum PartitionedAggregateFunction {
    Count,
    Sum,
    Min,
    Max,
    Avg,
    ApproxDistinct,
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

// enum storage for accumulator for fast static dispatching and easy translating between threads
#[derive(Debug, Clone)]
pub enum PartitionedAccumulatorEnum {
    Sum(PartitionedSumAccumulator),
}

impl PartitionedAccumulatorEnum {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        match self {
            PartitionedAccumulatorEnum::Sum(acc) => acc.state(),
        }
    }


    fn evaluate(&self) -> Result<ScalarValue> {
        match self {
            PartitionedAccumulatorEnum::Sum(acc) => acc.evaluate(),
        }
    }

    fn update_batch(&mut self, spans: &[bool], values: &[ArrayRef]) -> Result<()> {
        match self {
            PartitionedAccumulatorEnum::Sum(acc) => acc.update_batch(spans, values),
        }
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        match self {
            PartitionedAccumulatorEnum::Sum(acc) => acc.merge_batch(states),
        }
    }
}

// partitioned aggregate accumulator aggregates incoming partitioned values via acc accumulator and
// aggregates acc result via outer_acc
#[derive(Debug)]
pub struct PartitionedAggregateAccumulator {
    last_partition_value: ScalarValue,
    first_row: bool,
    acc: PartitionedAccumulatorEnum,
}

fn new_partitioned_accumulator(
    agg: &PartitionedAggregateFunction,
    outer_acc: AccumulatorEnum,
    data_type: &DataType,
) -> Result<PartitionedAccumulatorEnum> {
    Ok(match agg {
        PartitionedAggregateFunction::Sum => {
            PartitionedAccumulatorEnum::Sum(PartitionedSumAccumulator::try_new(data_type, outer_acc)?)
        }
        _ => unimplemented!(),
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
        let outer_acc = new_accumulator(outer_agg, agg_return_type)?;
        Ok(Self {
            last_partition_value: ScalarValue::try_from(partition_type)?,
            first_row: true,
            acc: new_partitioned_accumulator(agg, outer_acc, data_type)?,
        })
    }

    /*/// get the last value from acc and put it into outer_acc. This is called from state()
    fn finalize(&mut self) -> Result<()> {
        let res = self.acc.evaluate()?;
        self.outer_acc.update(&[res])
    }

    /// outer state
    fn outer_state(&self) -> Result<Vec<ScalarValue>> {
        self.outer_acc.state()
    }*/
}

impl Accumulator for PartitionedAggregateAccumulator {
    fn state(&self) -> DFResult<Vec<ScalarValue>> {
        self.acc.clone().state().map_err(Error::into_datafusion_execution_error)
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        let mut spans = Vec::with_capacity(values[0].len());

        match values[0].data_type() {
            DataType::Int8 => {
                let arr = values[0].as_any().downcast_ref::<Int8Array>().unwrap();
                let mut last_value: Option<i8> = if self.first_row {
                    self.first_row = false;
                    match arr.is_null(0) {
                        true => None,
                        false => Some(arr.value(0))
                    }
                } else {
                    match &self.last_partition_value {
                        ScalarValue::Int8(v) => *v,
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

                self.last_partition_value = ScalarValue::Int8(last_value);
            }
            _ => unimplemented!()
        }

        println!("{:?}", &spans);
        self.acc.update_batch(&spans, &values[1..]).map_err(Error::into_datafusion_execution_error);
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
