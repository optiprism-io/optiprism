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

use std::any::Any;
use std::convert::TryFrom;
use std::sync::Arc;

use arrow::compute;
use arrow::datatypes::DataType;
use arrow::{
    array::{
        ArrayRef, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
        Int8Array, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
    },
    datatypes::Field,
};
use datafusion::physical_plan::{Accumulator, AggregateExpr, PhysicalExpr};
use datafusion::physical_plan::expressions::{format_state_name};
use datafusion::scalar::ScalarValue;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_plan::{DFSchema, DFSchemaRef};
use datafusion::physical_plan::aggregates::AggregateFunction;
use datafusion::physical_plan::udaf::AggregateUDF;
use crate::physical_plan::expressions::average::AvgAccumulator;
use crate::physical_plan::expressions::sum::SumAccumulator;
use crate::physical_plan::expressions::count::CountAccumulator;
use crate::physical_plan::PartitionedAccumulator;
use crate::error::Result;
use crate::logical_plan::expr::Expr;

#[derive(Debug, Clone)]
pub struct PartitionedAggregateAccumulator {
    last_partition_value: ScalarValue,
    accum: Arc<dyn PartitionedAccumulator>,
    outer_accum: Arc<dyn PartitionedAccumulator>,
}

impl PartitionedAggregateAccumulator {
    /// new sum accumulator
    pub fn try_new(partition_type: &DataType, data_type: &DataType, agg: &AggregateFunction, outer_agg: &AggregateFunction) -> Result<Self> {
        let accum: Arc<dyn PartitionedAccumulator> = match agg {
            AggregateFunction::Sum => Arc::new(SumAccumulator::try_new(data_type)?),
            AggregateFunction::Avg => Arc::new(AvgAccumulator::try_new(data_type)?),
            AggregateFunction::Count => Arc::new(CountAccumulator::new()),
            _ => unimplemented!()
        };

        let outer_accum: Arc<dyn PartitionedAccumulator> = match outer_agg {
            AggregateFunction::Sum => Arc::new(SumAccumulator::try_new(data_type)?),
            AggregateFunction::Avg => Arc::new(AvgAccumulator::try_new(data_type)?),
            AggregateFunction::Count => Arc::new(CountAccumulator::new()),
            _ => unimplemented!()
        };

        Ok(Self {
            last_partition_value: ScalarValue::try_from(partition_type)?,
            accum,
            outer_accum,
        })
    }
}

impl Accumulator for PartitionedAggregateAccumulator {
    // this function serializes our state to `ScalarValue`, which DataFusion uses
    // to pass this state between execution stages.
    // Note that this can be arbitrary data.
    fn state(&self) -> DFResult<Vec<ScalarValue>> {
        self.outer_accum.state().map_err(|e| DataFusionError::Execution(e.to_string()))
    }

    // this function receives one entry per argument of this accumulator.
    // DataFusion calls this function on every row, and expects this function to update the accumulator's state.
    fn update(&mut self, values: &[ScalarValue]) -> DFResult<()> {
        println!("update: {:?}", values);
        Ok(())
    }

    // this function receives states from other accumulators (Vec<ScalarValue>)
    // and updates the accumulator.
    fn merge(&mut self, states: &[ScalarValue]) -> DFResult<()> {
        println!("merge: {:?}", states);
        Ok(())
    }

    fn evaluate(&self) -> DFResult<ScalarValue> {
        self.outer_accum.evaluate().map_err(|e| DataFusionError::Execution(e.to_string()))
    }
}
