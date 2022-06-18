use std::convert::TryFrom;
use std::fmt::Debug;
use std::ops::Add;
use std::sync::{Arc, Mutex, RwLock};

use crate::error::{Error, Result};
use crate::physical_plan::expressions::partitioned_aggregate::{
    Buffer, PartitionedAccumulator, PartitionedAggregate, Value,
};
use crate::DEFAULT_BATCH_SIZE;
use arrow::array::{
    Array, ArrayBuilder, ArrayRef, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    Int8Array, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::compute;
use arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion::physical_plan::aggregates::return_type;
use datafusion::physical_plan::expressions::{Avg, AvgAccumulator, Count, Literal, Max, Min, Sum};
use datafusion::physical_plan::{expressions, Accumulator, AggregateExpr, PhysicalExpr};
use datafusion::scalar::ScalarValue;

#[derive(Debug)]
pub struct PartitionedCountAccumulator {
    count: u64,
    buffer: Buffer,
}

impl PartitionedCountAccumulator {
    pub fn try_new(outer_acc: Box<dyn Accumulator>) -> Result<Self> {
        Ok(Self {
            count: 0,
            buffer: Buffer::new(DEFAULT_BATCH_SIZE, DataType::UInt64, outer_acc),
        })
    }
}

impl PartitionedAccumulator for PartitionedCountAccumulator {
    fn update_batch(&mut self, spans: &[bool], _: &[ArrayRef]) -> Result<()> {
        for (i, is_span) in spans.iter().enumerate() {
            if *is_span {
                self.buffer.push(self.count.into());
                self.count = 0;
            }
            self.count += 1;
        }

        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        Ok(self.buffer.merge_batch(states)?)
    }

    fn state(&self) -> Result<Vec<ScalarValue>> {
        self.buffer.flush_with_value(self.count.into())?;
        Ok(self.buffer.state()?)
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        self.buffer.flush_with_value(self.count.into())?;
        Ok(self.buffer.evaluate()?)
    }
}

#[cfg(test)]
mod tests {
    use crate::error::Result;
    use crate::physical_plan::expressions::partitioned_aggregate::PartitionedAccumulator;
    use crate::physical_plan::expressions::partitioned_count::PartitionedCountAccumulator;
    use crate::physical_plan::expressions::partitioned_sum::PartitionedSumAccumulator;
    use arrow::array::{ArrayRef, Int8Array};
    use arrow::datatypes::DataType;
    use datafusion::physical_plan::expressions::{Avg, AvgAccumulator, Literal};
    use datafusion::physical_plan::AggregateExpr;
    use datafusion::scalar::ScalarValue as DFScalarValue;
    use datafusion_common::ScalarValue;
    use datafusion_expr::Accumulator;
    use std::sync::Arc;

    #[test]
    fn test() -> Result<()> {
        let outer_acc: Box<dyn Accumulator> =
            Box::new(AvgAccumulator::try_new(&DataType::Float64)?);
        let mut count_acc = PartitionedCountAccumulator::try_new(outer_acc)?;
        let spans = vec![
            false, false, true, false, false, true, false, false, false, true,
        ];
        count_acc.update_batch(&spans, &vec![]);
        count_acc.update_batch(&spans, &vec![]);

        let list = vec![2, 3, 4, 3, 3, 4, 1];
        let sum: i32 = Iterator::sum(list.iter());
        let mean = f64::from(sum) / (list.len() as f64);

        assert_eq!(count_acc.evaluate()?, DFScalarValue::Float64(Some(mean)));

        Ok(())
    }
}
