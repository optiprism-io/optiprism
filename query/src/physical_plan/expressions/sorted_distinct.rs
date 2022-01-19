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
    distinct: u64,
}

impl SortedDistinctAccumulator {
    fn new() -> Self {
        Self::default()
    }

    fn offer(&mut self, value: &ScalarValue) {
        if self.current.is_none() || !value.eq(self.current.as_ref().unwrap()) {
            self.current = Some(value.clone());
            self.distinct += 1;
            // TODO return Err if data happens to be unsorted (detect asc/desc ordering on the fly)
        }
    }
}

impl Accumulator for SortedDistinctAccumulator {
    fn state(&self) -> datafusion::error::Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::UInt64(Some(self.distinct))])
    }

    fn update(&mut self, values: &[ScalarValue]) -> datafusion::error::Result<()> {
        for value in values {
            self.offer(value);
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