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

use crate::error::{Error, Result};
use crate::physical_plan::expressions::average::AvgAccumulator;
use crate::physical_plan::expressions::count::CountAccumulator;
use crate::physical_plan::expressions::sum::SumAccumulator;
use crate::physical_plan::PartitionedAccumulator;

use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;

use datafusion::physical_plan::aggregates::AggregateFunction;

use datafusion::physical_plan::{Accumulator, AggregateExpr, PhysicalExpr};
use datafusion::scalar::ScalarValue;

// enum storage for accumulator for fast static dispatching and easy translating between threads
#[derive(Debug, Clone)]
enum PartitionedAccumulatorEnum {
    Sum(SumAccumulator),
    Avg(AvgAccumulator),
    Count(CountAccumulator),
}

impl PartitionedAccumulator for PartitionedAccumulatorEnum {
    fn state(&self) -> Result<Vec<ScalarValue>> {
        match self {
            PartitionedAccumulatorEnum::Sum(acc) => acc.state(),
            PartitionedAccumulatorEnum::Avg(acc) => acc.state(),
            PartitionedAccumulatorEnum::Count(acc) => acc.state(),
        }
    }

    fn update(&mut self, values: &[ScalarValue]) -> Result<()> {
        match self {
            PartitionedAccumulatorEnum::Sum(acc) => acc.update(values),
            PartitionedAccumulatorEnum::Avg(acc) => acc.update(values),
            PartitionedAccumulatorEnum::Count(acc) => acc.update(values),
        }
    }
    fn merge(&mut self, states: &[ScalarValue]) -> Result<()> {
        match self {
            PartitionedAccumulatorEnum::Sum(acc) => acc.merge(states),
            PartitionedAccumulatorEnum::Avg(acc) => acc.merge(states),
            PartitionedAccumulatorEnum::Count(acc) => acc.merge(states),
        }
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        match self {
            PartitionedAccumulatorEnum::Sum(acc) => acc.evaluate(),
            PartitionedAccumulatorEnum::Avg(acc) => acc.evaluate(),
            PartitionedAccumulatorEnum::Count(acc) => acc.evaluate(),
        }
    }

    fn reset(&mut self) {
        match self {
            PartitionedAccumulatorEnum::Sum(acc) => acc.reset(),
            PartitionedAccumulatorEnum::Avg(acc) => acc.reset(),
            PartitionedAccumulatorEnum::Count(acc) => acc.reset(),
        }
    }
}

// partitioned aggregate is used as a accumulator factory from closure
pub struct PartitionedAggregate {
    partition_type: DataType,
    data_type: DataType,
    agg: AggregateFunction,
    outer_agg: AggregateFunction,
}

impl PartitionedAggregate {
    pub fn try_new(
        partition_type: DataType,
        data_type: DataType,
        agg: AggregateFunction,
        outer_agg: AggregateFunction,
    ) -> Result<Self> {
        Ok(Self {
            partition_type,
            data_type,
            agg,
            outer_agg,
        })
    }

    pub fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(PartitionedAggregateAccumulator::try_new(
            &self.partition_type,
            &self.data_type,
            &self.agg,
            &self.outer_agg,
        )?))
    }
}

// partitioned aggregate accumulator aggregates incoming partitioned values via acc accumulator and
// aggregates acc result via outer_acc
#[derive(Debug, Clone)]
pub struct PartitionedAggregateAccumulator {
    last_partition_value: ScalarValue,
    first_row: bool,
    acc: PartitionedAccumulatorEnum,
    outer_acc: PartitionedAccumulatorEnum,
}

fn new_accumulator(
    agg: &AggregateFunction,
    data_type: &DataType,
) -> Result<PartitionedAccumulatorEnum> {
    Ok(match agg {
        AggregateFunction::Sum => {
            PartitionedAccumulatorEnum::Sum(SumAccumulator::try_new(data_type)?)
        }
        AggregateFunction::Avg => {
            PartitionedAccumulatorEnum::Avg(AvgAccumulator::try_new(data_type)?)
        }
        AggregateFunction::Count => PartitionedAccumulatorEnum::Count(CountAccumulator::new()),
        _ => unimplemented!(),
    })
}

pub fn state_types(data_type: DataType, agg: &AggregateFunction) -> Result<Vec<DataType>> {
    Ok(match agg {
        AggregateFunction::Count => vec![DataType::UInt64],
        AggregateFunction::Sum => vec![data_type.clone()],
        AggregateFunction::Avg => vec![DataType::UInt64, data_type.clone()],
        _ => unimplemented!(),
    })
}

impl PartitionedAggregateAccumulator {
    /// new sum accumulator
    pub fn try_new(
        partition_type: &DataType,
        data_type: &DataType,
        agg: &AggregateFunction,
        outer_agg: &AggregateFunction,
    ) -> Result<Self> {
        Ok(Self {
            last_partition_value: ScalarValue::try_from(partition_type)?,
            first_row: true,
            acc: new_accumulator(agg, data_type)?,
            outer_acc: new_accumulator(outer_agg, data_type)?,
        })
    }

    // get the last value from acc and put it into outer_acc. This is called from state()
    fn finalize(&mut self) -> Result<()> {
        let res = self.acc.evaluate()?;
        self.outer_acc.update(&[res])
    }

    // outer state
    fn outer_state(&self) -> Result<Vec<ScalarValue>> {
        self.outer_acc.state()
    }
}

impl Accumulator for PartitionedAggregateAccumulator {
    // this function finalizes and serializes our state to `ScalarValue`, which DataFusion uses
    // to pass this state between execution stages.
    // Note that this can be arbitrary data.
    fn state(&self) -> DFResult<Vec<ScalarValue>> {
        // we should clone the accumulator because state is immutable and we can't simply mutate it
        let mut outer_acc = self.clone();
        outer_acc
            .finalize()
            .map_err(Error::into_datafusion_execution_error)?;
        return outer_acc
            .outer_state()
            .map_err(Error::into_datafusion_execution_error);
    }

    // this function receives one entry per argument of this accumulator.
    // DataFusion calls this function on every row, and expects this function to update the accumulator's state.
    // TODO: leverage update_batch
    fn update(&mut self, values: &[ScalarValue]) -> DFResult<()> {
        if self.first_row {
            self.last_partition_value = values[0].clone();
            self.first_row = false
        }

        match self.last_partition_value.partial_cmp(&values[0]) {
            None => unreachable!(),
            Some(ord) => match ord {
                Ordering::Less | Ordering::Greater => {
                    let res = self
                        .acc
                        .evaluate()
                        .map_err(Error::into_datafusion_execution_error)?;
                    self.outer_acc
                        .update(&[res])
                        .map_err(Error::into_datafusion_execution_error)?;
                    self.acc.reset();
                    self.last_partition_value = values[0].clone();
                }

                _ => {}
            },
        };

        self.acc
            .update(&values[1..])
            .map_err(Error::into_datafusion_execution_error)?;
        Ok(())
    }

    // this function receives states from other accumulators (Vec<ScalarValue>)
    // and updates the accumulator.
    fn merge(&mut self, states: &[ScalarValue]) -> DFResult<()> {
        self.outer_acc
            .merge(states)
            .map_err(Error::into_datafusion_execution_error)
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        self.outer_acc
            .merge_batch(states)
            .map_err(Error::into_datafusion_execution_error)
    }

    fn evaluate(&self) -> DFResult<ScalarValue> {
        self.outer_acc
            .evaluate()
            .map_err(Error::into_datafusion_execution_error)
    }
}
