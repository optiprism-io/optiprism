use std::fmt::Debug;
use arrow::array::ArrayRef;
use datafusion::scalar::ScalarValue;
use crate::error::Result;
use datafusion::error::{DataFusionError, Result as DFResult};

pub mod expressions;

pub trait PartitionedAccumulator: Debug + Send + Sync {
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

    /// returns its value based on its current state.
    fn evaluate(&self) -> Result<ScalarValue>;
    fn reset(&mut self);
}