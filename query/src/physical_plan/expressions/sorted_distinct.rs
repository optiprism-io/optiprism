use std::cmp::Ordering;
use std::fmt::Debug;
use std::sync::Arc;
use arrow::datatypes::DataType;
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

impl From<SortedDistinct> for AggregateUDF {
    fn from(sorted_distinct: SortedDistinct) -> Self {
        let data_type = Arc::new(sorted_distinct.data_type.clone());
        let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(data_type.clone()));
        let accumulator: AccumulatorFunctionImplementation = Arc::new(|| Ok(Box::new(SortedDistinctAccumulator::new())));
        let state_type: StateTypeFunction = Arc::new(|_| Ok(Arc::new(vec![DataType::UInt64])));
        AggregateUDF::new(
            &sorted_distinct.name,
            &Signature::new(
                TypeSignature::Variadic(vec![sorted_distinct.data_type]),
                Volatility::Immutable,
            ),
            &return_type,
            &accumulator,
            &state_type,
        )
    }
}

#[derive(Debug, Default)]
struct SortedDistinctAccumulator {
    current: Option<ScalarValue>,
    ordering: Option<Ordering>,
    distinct: u64,
}

impl SortedDistinctAccumulator {
    fn new() -> Self {
        Self::default()
    }

    fn offer(&mut self, value: &ScalarValue) -> datafusion::error::Result<()> {
        if value.is_null() {
            return Ok(());
        }

        if self.current.is_none() {
            self.current = Some(value.clone());
            self.distinct += 1;
            return Ok(());
        }

        let ordering = value.partial_cmp(self.current.as_ref().unwrap());
        if ordering.is_none() {
            let message = format!("SortedDistinctAccumulator: cannot compare '{}' to '{}'.",
                                  value, self.current.as_ref().unwrap());
            return Err(datafusion::error::DataFusionError::Internal(message));
        }
        let ordering = ordering.unwrap();

        if self.ordering.is_none() && ordering != Ordering::Equal {
            self.distinct += 1;
            self.current = Some(value.clone());
            self.ordering = Some(ordering);
            return Ok(());
        }

        if ordering == Ordering::Equal {
            return Ok(());
        } else {
            self.distinct += 1;
            self.current = Some(value.clone());
        }

        if &ordering != self.ordering.as_ref().unwrap() {
            let message = format!("SortedDistinctAccumulator: ordering violation detected '{:?}' after '{:?}'.",
                                  ordering, self.ordering.as_ref().unwrap());
            return Err(datafusion::error::DataFusionError::Internal(message));
        }

        Ok(())
    }
}

impl Accumulator for SortedDistinctAccumulator {
    fn state(&self) -> datafusion::error::Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::UInt64(Some(self.distinct))])
    }

    fn update(&mut self, values: &[ScalarValue]) -> datafusion::error::Result<()> {
        for value in values {
            self.offer(value)?;
        }
        Ok(())
    }

    fn merge(&mut self, states: &[ScalarValue]) -> datafusion::error::Result<()> {
        for state in states {
            if let ScalarValue::UInt64(Some(distinct)) = state {
                self.distinct += *distinct;
            } else {
                let message = format!("Invalid state of SortedDistinctAccumulator: {}", state);
                return Err(datafusion::error::DataFusionError::Internal(message));
            }
        }
        Ok(())
    }

    fn evaluate(&self) -> datafusion::error::Result<ScalarValue> {
        Ok(ScalarValue::UInt64(Some(self.distinct)))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use datafusion::error::DataFusionError;
    use super::*;

    fn check_distinct(sequence: &[i64], expected: usize) -> datafusion::error::Result<()> {
        let mut acc = SortedDistinctAccumulator::new();
        for val in sequence {
            acc.offer(&ScalarValue::Int64(Some(*val)))?;
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

    #[test]
    fn test_unordered() {
        let sequence = vec![1, 2, 3, 5, 7, 11, 13, 17, 19, 1];
        let result = check_distinct(&sequence, sequence.len());

        let expected = "SortedDistinctAccumulator: ordering violation detected 'Less' after 'Greater'.";
        match result {
            Err(DataFusionError::Internal(actual)) => assert_eq!(actual, expected),
            e => panic!("Unexpected result: {:?}", e)
        }
    }

}
