use std::cmp::Ordering;
use std::fmt::Debug;
use std::sync::Arc;
use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::{Accumulator, AggregateExpr, PhysicalExpr};
use datafusion::physical_plan::aggregates::{AccumulatorFunctionImplementation, StateTypeFunction};
use datafusion::physical_plan::functions::{ReturnTypeFunction, Signature, TypeSignature, Volatility};
use datafusion::physical_plan::udaf::AggregateUDF;
use datafusion::scalar::ScalarValue;

#[derive(Debug)]
pub struct SortedDistinct {
    name: String,
    data_type: DataType,
}

impl SortedDistinct {
    pub fn new(
        name: String,
        data_type: DataType,
    ) -> Self {
        Self {
            name,
            data_type,
        }
    }
}

impl TryFrom<SortedDistinct> for AggregateUDF {
    type Error = DataFusionError;

    fn try_from(sorted_distinct: SortedDistinct) -> std::result::Result<Self, Self::Error> {
        let data_type = sorted_distinct.data_type.clone();
        let data_type_arc = Arc::new(data_type.clone());
        let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(data_type_arc.clone()));
        let accumulator: AccumulatorFunctionImplementation = Arc::new(
            move || {
                let acc = SortedDistinctCountAccumulator::try_new(&data_type)?;
                Ok(Box::new(acc))
            });
        let state_type: StateTypeFunction = Arc::new(|_| Ok(Arc::new(vec![DataType::UInt64])));
        Ok(AggregateUDF::new(
            &sorted_distinct.name,
            &Signature::new(
                TypeSignature::Any(1),
                Volatility::Immutable,
            ),
            &return_type,
            &accumulator,
            &state_type,
        ))
    }
}

#[derive(Debug)]
struct SortedDistinctCountAccumulator {
    current: ScalarValue,
    count: u64,
}

impl SortedDistinctCountAccumulator {
    fn try_new(data_type: &DataType) -> Result<Self> {
        let current = ScalarValue::try_from(data_type)?;
        Ok(Self {
            current,
            count: 0,
        })
    }
}

impl Accumulator for SortedDistinctCountAccumulator {
    fn state(&self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::UInt64(Some(self.count))])
    }

    fn update(&mut self, values: &[ScalarValue]) -> Result<()> {
        let value = &values[0];
        if !self.current.eq(value) {
            self.current = value.clone();
            self.count += 1;
        }
        Ok(())
    }

    fn merge(&mut self, states: &[ScalarValue]) -> Result<()> {
        for state in states {
            if let ScalarValue::UInt64(Some(distinct)) = state {
                self.count += *distinct;
            }
        }
        Ok(())
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        Ok(ScalarValue::UInt64(Some(self.count)))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use super::*;

    fn check_distinct(sequence: &[i64], expected: usize) -> datafusion::error::Result<()> {
        let mut acc = SortedDistinctCountAccumulator::try_new(&DataType::Int64)?;
        for val in sequence {
            acc.update(&[ScalarValue::Int64(Some(*val))])?;
        }
        let state = acc.state()?[0].clone();
        assert_eq!(state, ScalarValue::UInt64(Some(expected as u64)));
        Ok(())
    }

    #[test]
    fn test_ordered_unique() {
        let sequence = vec![1, 2, 3, 5, 7, 11, 13, 17, 19];
        check_distinct(&sequence, sequence.len()).unwrap();
    }

    #[test]
    fn test_ordered_duplicates() {
        let sequence = vec![1, 2, 2, 3, 3, 3, 5, 5, 5, 5, 7, 7, 7, 11, 13, 17, 19];
        let expected = sequence.iter().cloned().collect::<HashSet<_>>().len();
        check_distinct(&sequence, expected).unwrap();
    }
}
