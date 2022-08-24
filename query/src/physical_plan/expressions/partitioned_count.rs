use std::fmt::Debug;

use crate::error::Result;
use crate::physical_plan::expressions::partitioned_aggregate::{Buffer, PartitionedAccumulator};
use crate::DEFAULT_BATCH_SIZE;
use arrow::array::ArrayRef;

use arrow::datatypes::DataType;

use datafusion::physical_plan::Accumulator;
use datafusion_common::ScalarValue;
use datafusion_expr::AggregateState;

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
        for (_i, is_span) in spans.iter().enumerate() {
            if *is_span {
                self.buffer.push(self.count.into())?;
                self.count = 0;
            }
            self.count += 1;
        }

        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        Ok(self.buffer.merge_batch(states)?)
    }

    fn state(&self) -> Result<Vec<AggregateState>> {
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

    use arrow::datatypes::DataType;
    use datafusion::physical_plan::expressions::AvgAccumulator;

    use datafusion_common::ScalarValue as DFScalarValue;

    use datafusion_expr::Accumulator;

    #[test]
    fn test() -> Result<()> {
        let outer_acc: Box<dyn Accumulator> =
            Box::new(AvgAccumulator::try_new(&DataType::Float64)?);
        let mut count_acc = PartitionedCountAccumulator::try_new(outer_acc)?;
        let spans = vec![
            false, false, true, false, false, true, false, false, false, true,
        ];
        count_acc.update_batch(&spans, &[]);
        count_acc.update_batch(&spans, &[]);

        let list = vec![2, 3, 4, 3, 3, 4, 1];
        let sum: i32 = Iterator::sum(list.iter());
        let mean = f64::from(sum) / (list.len() as f64);

        assert_eq!(count_acc.evaluate()?, DFScalarValue::Float64(Some(mean)));

        Ok(())
    }
}
