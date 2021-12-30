use crate::error::Result;
use arrow::array::ArrayRef;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::scalar::ScalarValue;
use std::fmt::Debug;

pub mod expressions;

// PartitionedAccumulator extends Accumulator trait with reset
pub trait PartitionedAccumulator: Debug {
    /// Returns the state of the accumulator at the end of the accumulation.
    // in the case of an average on which we track `sum` and `n`, this function should return a vector
    // of two values, sum and n.
    fn state(&self) -> Result<Vec<ScalarValue>>;

    /// updates the accumulator's state from a vector of scalars.
    fn update(&mut self, values: &[ScalarValue]) -> Result<()>;

    /// updates the accumulator's state from a vector of arrays.
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        };
        (0..values[0].len()).try_for_each(|index| {
            let v = values
                .iter()
                .map(|array| ScalarValue::try_from_array(array, index))
                .collect::<DFResult<Vec<_>>>()?;
            self.update(&v)
        })
    }

    /// updates the accumulator's state from a vector of scalars.
    fn merge(&mut self, states: &[ScalarValue]) -> Result<()>;

    /// updates the accumulator's state from a vector of states.
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        };
        (0..states[0].len()).try_for_each(|index| {
            let v = states
                .iter()
                .map(|array| ScalarValue::try_from_array(array, index))
                .collect::<DFResult<Vec<_>>>()?;
            self.merge(&v)
        })
    }

    /// returns its value based on its current state.
    fn evaluate(&self) -> Result<ScalarValue>;
    // resets accumulator state
    fn reset(&mut self);
}
