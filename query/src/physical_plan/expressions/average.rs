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

use std::convert::TryFrom;

use crate::error::{Error, Result};
use crate::physical_plan::expressions::sum::{sum, sum_batch};
use crate::physical_plan::PartitionedAccumulator;
use arrow::array::{ArrayRef, UInt64Array};
use arrow::compute;
use datafusion::arrow::datatypes::DataType;

use datafusion::scalar::ScalarValue;

/// An accumulator to compute the average
#[derive(Debug, Clone)]
pub struct AvgAccumulator {
    // sum is used for null
    sum: ScalarValue,
    count: u64,
}

impl AvgAccumulator {
    /// Creates a new `AvgAccumulator`
    pub fn try_new(data_type: &DataType) -> Result<Self> {
        Ok(Self {
            sum: ScalarValue::try_from(data_type)?,
            count: 0,
        })
    }
}

impl PartitionedAccumulator for AvgAccumulator {
    fn state(&self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::from(self.count), self.sum.clone()])
    }

    fn update(&mut self, values: &[ScalarValue]) -> Result<()> {
        let values = &values[0];

        self.count += (!values.is_null()) as u64;
        self.sum = sum(&self.sum, values)?;

        Ok(())
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = &values[0];

        self.count += (values.len() - values.data().null_count()) as u64;
        self.sum = sum(&self.sum, &sum_batch(values)?)?;
        Ok(())
    }

    fn merge(&mut self, states: &[ScalarValue]) -> Result<()> {
        let count = &states[0];
        // counts are summed
        if let ScalarValue::UInt64(Some(c)) = count {
            self.count += c
        } else {
            unreachable!()
        };

        // sums are summed
        self.sum = sum(&self.sum, &states[1])?;
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let counts = states[0].as_any().downcast_ref::<UInt64Array>().unwrap();
        // counts are summed
        self.count += compute::sum(counts).unwrap_or(0);

        // sums are summed
        self.sum = sum(&self.sum, &sum_batch(&states[1])?)?;
        Ok(())
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        match self.sum {
            ScalarValue::Float64(e) => Ok(ScalarValue::Float64(e.map(|f| f / self.count as f64))),
            _ => Err(Error::Internal("Sum should be f64 on average".to_string())),
        }
    }

    fn reset(&mut self) -> Result<()> {
        self.sum = ScalarValue::try_from(&self.sum.get_datatype())?;
        self.count = 0;
        Ok(())
    }
}
