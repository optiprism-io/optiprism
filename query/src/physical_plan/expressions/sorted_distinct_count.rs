use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Date64Array, Int16Array, Int32Array, Int64Array,
    Int8Array, StringArray, TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array,
    UInt8Array,
};
use arrow::compute;
use arrow::datatypes::{DataType, TimeUnit};
use datafusion_common::error::{DataFusionError, Result};
use datafusion::physical_plan::functions::{Signature, TypeSignature, Volatility};
use datafusion::physical_plan::udaf::AggregateUDF;
use datafusion::physical_plan::Accumulator;
use datafusion_common::ScalarValue;
use datafusion_expr::{AccumulatorFunctionImplementation, ReturnTypeFunction, StateTypeFunction};
use std::fmt::Debug;
use std::sync::Arc;

#[derive(Debug)]
pub struct SortedDistinctCount {
    name: String,
    data_type: DataType,
}

impl SortedDistinctCount {
    pub fn new(name: String, data_type: DataType) -> Self {
        Self { name, data_type }
    }
}

impl TryFrom<SortedDistinctCount> for AggregateUDF {
    type Error = DataFusionError;

    fn try_from(sorted_distinct: SortedDistinctCount) -> std::result::Result<Self, Self::Error> {
        let data_type = sorted_distinct.data_type.clone();
        let data_type_arc = Arc::new(data_type.clone());
        let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(data_type_arc.clone()));
        let accumulator: AccumulatorFunctionImplementation = Arc::new(move || {
            let acc = SortedDistinctCountAccumulator::try_new(&data_type)?;
            Ok(Box::new(acc))
        });
        let state_type: StateTypeFunction = Arc::new(|_| Ok(Arc::new(vec![DataType::UInt64])));
        Ok(AggregateUDF::new(
            &sorted_distinct.name,
            &Signature::new(TypeSignature::Any(1), Volatility::Immutable),
            &return_type,
            &accumulator,
            &state_type,
        ))
    }
}

#[derive(Debug)]
pub struct SortedDistinctCountAccumulator {
    current: ScalarValue,
    count: u64,
    full: bool,
}

impl SortedDistinctCountAccumulator {
    pub fn try_new(data_type: &DataType) -> Result<Self> {
        let current = ScalarValue::try_from(data_type)?;
        Ok(Self {
            current,
            count: 0,
            full: false,
        })
    }

    pub fn count(&self) -> u64 {
        self.count
    }

    pub fn reset(&mut self) {
        let zero = ScalarValue::try_from(&self.current.get_datatype()).unwrap();
        self.current = zero;
        self.count = 0;
        self.full = false;
    }
}

macro_rules! distinct_count_array {
    ($array:expr, $ARRAYTYPE:ident, $state:expr) => {{
        let array_size = $array.len();
        let typed_array = $array.as_any().downcast_ref::<$ARRAYTYPE>().unwrap();
        for index in 0..array_size {
            if !$state.current.eq_array($array, index) {
                $state.current = typed_array.value(index).into();
                $state.count += 1;
            }
        }
        Ok(())
    }};
}

macro_rules! distinct_count_array_limited {
    ($array:expr, $ARRAYTYPE:ident, $state:expr, $limit:expr) => {{
        if $state.full {
            return Ok(());
        }
        if $state.count >= $limit {
            $state.full = true;
            return Ok(());
        }
        let array_size = $array.len();
        let typed_array = $array.as_any().downcast_ref::<$ARRAYTYPE>().unwrap();
        for index in 0..array_size {
            if !$state.current.eq_array($array, index) {
                $state.current = typed_array.value(index).into();
                $state.count += 1;
                if $state.count >= $limit {
                    $state.full = true;
                    return Ok(());
                }
            }
        }
        Ok(())
    }};
}

fn distinct_count(array: &ArrayRef, state: &mut SortedDistinctCountAccumulator) -> Result<()> {
    if state.full {
        return Ok(());
    }
    match array.data_type() {
        DataType::Boolean => distinct_count_array_limited!(array, BooleanArray, state, 2),
        DataType::Utf8 => distinct_count_array!(array, StringArray, state),
        DataType::UInt64 => distinct_count_array!(array, UInt64Array, state),
        DataType::UInt32 => distinct_count_array!(array, UInt32Array, state),
        DataType::UInt16 => distinct_count_array!(array, UInt16Array, state),
        DataType::UInt8 => distinct_count_array_limited!(array, UInt8Array, state, 256),
        DataType::Int64 => distinct_count_array!(array, Int64Array, state),
        DataType::Int32 => distinct_count_array!(array, Int32Array, state),
        DataType::Int16 => distinct_count_array!(array, Int16Array, state),
        DataType::Int8 => distinct_count_array_limited!(array, Int8Array, state, 256),
        DataType::Date32 => distinct_count_array!(array, Date32Array, state),
        DataType::Date64 => distinct_count_array!(array, Date64Array, state),
        DataType::Timestamp(TimeUnit::Second, _) => {
            distinct_count_array!(array, TimestampSecondArray, state)
        }

        // "the trait `From<i128>` is not implemented for `datafusion_common::ScalarValue`"
        // TODO Enable once https://github.com/apache/arrow-datafusion/pull/1394 is released
        // DataType::Decimal128(_, _) => distinct_count_array!(array, DecimalArray, state),
        other => {
            let message = format!(
                "Ordered distinct count over array of type \"{:?}\" is not supported",
                other
            );
            Err(DataFusionError::NotImplemented(message))
        }
    }
}

impl Accumulator for SortedDistinctCountAccumulator {
    fn state(&self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::UInt64(Some(self.count))])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        distinct_count(&values[0], self)
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let counts = states[0].as_any().downcast_ref::<UInt64Array>().unwrap();
        let delta = &compute::sum(counts);
        if let Some(d) = delta {
            self.count += *d;
        }
        Ok(())
    }
    fn evaluate(&self) -> Result<ScalarValue> {
        Ok(ScalarValue::UInt64(Some(self.count)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check_batch(sequences: &[Vec<i64>], expected: usize) -> datafusion_common::error::Result<()> {
        let mut acc = SortedDistinctCountAccumulator::try_new(&DataType::Int64)?;
        for seq in sequences {
            let array = Int64Array::from(seq.to_owned());
            let array_ref = Arc::new(array);
            acc.update_batch(&[array_ref])?;
        }
        let state = acc.state()?[0].clone();
        assert_eq!(state, ScalarValue::UInt64(Some(expected as u64)));
        Ok(())
    }

    #[test]
    fn test_batch_ordered_disjoint() {
        check_batch(
            &[
                vec![1, 1, 1, 2, 2, 3, 3, 3],
                vec![4, 4, 5, 5, 5, 6, 6, 6],
                vec![7, 7, 7, 8, 8, 9, 9, 9],
            ],
            9,
        )
        .unwrap();
    }

    #[test]
    fn test_batch_ordered_overlapped() {
        check_batch(
            &[
                vec![1, 1, 2, 2, 3, 3, 4, 4, 4], // value 4 overlaps with next batch
                vec![4, 4, 4, 5, 5, 5, 6, 7, 7], // value 7 overlaps with next batch
                vec![7, 7, 7, 8, 8, 8, 9, 9, 9],
            ],
            9,
        )
        .unwrap();
    }
}
