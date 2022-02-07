use std::fmt::Debug;
use std::sync::Arc;
use arrow::array::{Array, ArrayRef, BooleanArray, Date32Array, Date64Array, DecimalArray, Int16Array, Int32Array, Int64Array, Int8Array, StringArray, TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array};
use arrow::datatypes::{DataType, TimeUnit};
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::Accumulator;
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

#[derive(Clone, Debug)]
enum Value {
    Decimal(Option<i128>),
    Scalar(ScalarValue),
}

struct Decimal(i128);

impl From<i128> for Decimal {
    fn from(decimal: i128) -> Self {
        Decimal(decimal)
    }
}

impl From<Decimal> for Value {
    fn from(decimal: Decimal) -> Self {
        Self::Decimal(Some(decimal.0))
    }
}

impl<T> From<T> for Value
    where
        T: Into<ScalarValue>
{
    fn from(scalar: T) -> Self {
        Self::Scalar(scalar.into())
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Scalar(this), Value::Scalar(that)) => this.eq(that),
            (Value::Decimal(this), Value::Decimal(that)) => {
                match (this, that) {
                    (Some(a), Some(b)) => a.eq(b),
                    (None, None) => true,
                    _ => false
                }
            },
            _ => false
        }
    }
}

impl Eq for Value {}

#[derive(Debug)]
struct SortedDistinctCountAccumulator {
    current: Value,
    count: u64,
}

impl SortedDistinctCountAccumulator {
    fn try_new(data_type: &DataType) -> Result<Self> {
        let current = match ScalarValue::try_from(data_type) {
            Ok(scalar) => Value::Scalar(scalar),
            Err(_) => Value::Decimal(None)
        };
        Ok(Self {
            current,
            count: 0,
        })
    }
}

// Decimal cannot be represented as a ScalarValue due to missing impl From<i128> for ScalarValue
// TODO FIXME Decimal support gets introduced into ScalarValue in https://github.com/apache/arrow-datafusion/pull/1394
fn distinct_count_array_decimal(array: &ArrayRef, state: &mut SortedDistinctCountAccumulator) -> Result<()> {
    let array = array.as_any().downcast_ref::<DecimalArray>().unwrap();
    for index in 0..array.len() {
        let value: Value = Decimal(array.value(index)).into();
        if !value.eq(&state.current) {
            state.current = value;
            state.count += 1;
        }
    }
    Ok(())
}

macro_rules! distinct_count_array {
    ($ARRAYREF:expr, $ARRAYTYPE:ident, $STATE:expr) => {{
        let array = $ARRAYREF.as_any().downcast_ref::<$ARRAYTYPE>().unwrap();
        for index in 0..array.len() {
            let value: Value = array.value(index).into();
            if !value.eq(&$STATE.current) {
                $STATE.current = value;
                $STATE.count += 1;
            }
        }
        Ok(())
    }};
}

macro_rules! distinct_count_array_limited {
    ($ARRAYREF:expr, $ARRAYTYPE:ident, $STATE:expr, $LIMIT:expr) => {{
        let array = $ARRAYREF.as_any().downcast_ref::<$ARRAYTYPE>().unwrap();
        for index in 0..array.len() {
            let value: Value = array.value(index).into();
            if !value.eq(&$STATE.current) {
                $STATE.current = value;
                $STATE.count += 1;
                if $STATE.count >= $LIMIT {
                    return Ok(());
                }
            }
        }
        Ok(())
    }};
}

fn distinct_count(array: &ArrayRef, state: &mut SortedDistinctCountAccumulator) -> Result<()> {
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
        DataType::Decimal(_, _) => distinct_count_array_decimal(array, state),
        DataType::Timestamp(TimeUnit::Second, _) => distinct_count_array!(array, TimestampSecondArray, state),
        other => {
            let message = format!("Ordered distinct count over array of type \"{:?}\" is not supported", other);
            Err(DataFusionError::NotImplemented(message))
        }
    }
}

impl Accumulator for SortedDistinctCountAccumulator {
    fn state(&self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::UInt64(Some(self.count))])
    }

    fn update(&mut self, values: &[ScalarValue]) -> Result<()> {
        let value = values[0].clone().into();
        if !self.current.eq(&value) {
            self.current = value;
            self.count += 1;
        }
        Ok(())
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        distinct_count(&values[0], self)
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

    fn check_update(sequence: &[i64], expected: usize) -> datafusion::error::Result<()> {
        let mut acc = SortedDistinctCountAccumulator::try_new(&DataType::Int64)?;
        for val in sequence {
            acc.update(&[ScalarValue::Int64(Some(*val))])?;
        }
        let state = acc.state()?[0].clone();
        assert_eq!(state, ScalarValue::UInt64(Some(expected as u64)));
        Ok(())
    }

    fn check_batch(sequences: &[Vec<i64>], expected: usize) -> datafusion::error::Result<()> {
        let mut acc = SortedDistinctCountAccumulator::try_new(&DataType::Int64)?;
        for seq in sequences {
            let array = Int64Array::from(seq.to_owned());
            let array_ref= Arc::new(array);
            acc.update_batch(&[array_ref])?;
        }
        let state = acc.state()?[0].clone();
        assert_eq!(state, ScalarValue::UInt64(Some(expected as u64)));
        Ok(())
    }

    #[test]
    fn test_update_ordered_unique() {
        let sequence = vec![1, 2, 3, 5, 7, 11, 13, 17, 19];
        check_update(&sequence, sequence.len()).unwrap();
    }

    #[test]
    fn test_update_ordered_duplicates() {
        let sequence = vec![1, 2, 2, 3, 3, 3, 5, 5, 5, 5, 7, 7, 7, 11, 13, 17, 19];
        let expected = sequence.iter().cloned().collect::<HashSet<_>>().len();
        check_update(&sequence, expected).unwrap();
    }

    #[test]
    fn test_batch_ordered_disjoint() {
        check_batch(&[
            vec![1, 1, 1, 2, 2, 3, 3, 3],
            vec![4, 4, 5, 5, 5, 6, 6, 6],
            vec![7, 7, 7, 8, 8, 9, 9, 9]], 9).unwrap();
    }

    #[test]
    fn test_batch_ordered_overlapped() {
        check_batch(&[
            vec![1, 1, 2, 2, 3, 3, 4, 4, 4], // value 4 overlaps with next batch
            vec![4, 4, 4, 5, 5, 5, 6, 7, 7], // value 7 overlaps with next batch
            vec![7, 7, 7, 8, 8, 8, 9, 9, 9]], 9).unwrap();
    }
}
