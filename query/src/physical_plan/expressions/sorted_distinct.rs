use std::cmp::Ordering;
use std::fmt::Debug;
use std::sync::Arc;
use arrow::array::{ArrayRef, BinaryArray, BooleanArray, Date32Array, Date64Array, DictionaryArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, LargeBinaryArray, LargeStringArray, ListArray, PrimitiveArray, StringArray, StructArray, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array};
use arrow::datatypes::{ArrowPrimitiveType, DataType, Int64Type, Int8Type, TimeUnit};
use datafusion::error::{DataFusionError, Result};
use datafusion::parquet::arrow::converter::Utf8ArrayConverter;
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

fn distinct_iter_count<T, I>(it: I) -> u64
    where
        I: Iterator<Item=Option<T>>,
        T: Clone + Eq
{
    let mut current: Option<T> = None;
    let mut count: u64 = 0;
    for item in it {
        if item.is_none() {
            continue;
        }
        let item = item.as_ref().unwrap();
        if current.is_none() || !current.as_ref().unwrap().eq(&item) {
            count += 1;
            current = Some(item.clone());
        }
    }
    count
}

fn distinct_array_count<A, T>(array: &ArrayRef, data_type: &DataType) -> Result<u64>
    where
        A: 'static,
        T: Clone + Eq,
        for<'a> &'a A: IntoIterator<Item = Option<T>>,
{
    if let Some(arr) = array.as_any().downcast_ref::<A>() {
        let it = arr.into_iter();
        Ok(distinct_iter_count(it))
    } else {
        let message = format!("Failed to downcast array of type '{}' to type item type '{}",
            array.data_type(), data_type);
        Err(DataFusionError::Internal(message))
    }
}

fn distinct_count(array: &ArrayRef) -> Result<u64> {
    let dt = array.data_type();
    match dt {
        DataType::Boolean => distinct_array_count::<BooleanArray, _>(array, dt),
        // TODO FIXME Float{32, 64} are not Eq
        // DataType::Float64 => distinct_array_count::<Float64Array, _>(array, dt),
        // DataType::Float32 => distinct_array_count::<Float32Array, _>(array, dt),
        DataType::UInt64 => distinct_array_count::<UInt64Array, _>(array, dt),
        DataType::UInt32 => distinct_array_count::<UInt32Array, _>(array, dt),
        DataType::UInt16 => distinct_array_count::<UInt16Array, _>(array, dt),
        DataType::UInt8 => distinct_array_count::<UInt8Array, _>(array, dt),
        DataType::Int64 => distinct_array_count::<Int64Array, _>(array, dt),
        DataType::Int32 => distinct_array_count::<Int32Array, _>(array, dt),
        DataType::Int16 => distinct_array_count::<Int16Array, _>(array, dt),
        DataType::Int8 => distinct_array_count::<Int8Array, _>(array, dt),
        // TODO FIXME implementation of `IntoIterator` is not general enough
        // DataType::Binary => distinct_array_count::<BinaryArray, _>(array, dt),
        // DataType::LargeBinary => distinct_array_count::<LargeBinaryArray, _>(array, dt),
        // DataType::Utf8 => distinct_array_count::<StringArray, _>(array, dt),
        // DataType::LargeUtf8 => distinct_array_count::<LargeStringArray, _>(array, dt),
        // TODO FIXME ListArray is not an iterator
        // DataType::List(_) => distinct_array_count::<ListArray, _>(array, dt),
        DataType::Date32 => distinct_array_count::<Date32Array, _>(array, dt),
        DataType::Date64 => distinct_array_count::<Date64Array, _>(array, dt),
        DataType::Timestamp(TimeUnit::Second, _) => distinct_array_count::<TimestampSecondArray, _>(array, dt),
        DataType::Timestamp(TimeUnit::Millisecond, _) => distinct_array_count::<TimestampMillisecondArray, _>(array, dt),
        DataType::Timestamp(TimeUnit::Microsecond, _) => distinct_array_count::<TimestampMicrosecondArray, _>(array, dt),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => distinct_array_count::<TimestampNanosecondArray, _>(array, dt),
        DataType::Dictionary(index_type, _) => {
            let index_type = *index_type.clone();
            match index_type {
                // TODO FIXME DictionaryArray is not an iterator
                //DataType::Int8 => distinct_array_count::<DictionaryArray<Int8Type>, _>(array, dt),
                other => {
                    let message = format!("Cannot compute ordered distinct count over array of dictionary[{:?}]", other);
                    Err(DataFusionError::NotImplemented(message))
                }
            }
        },
        // TODO FIXME StructArray is not an iterator
        //DataType::Struct(_) => distinct_array_count::<StructArray, _>(array, dt),
        other => {
            let message = format!("Cannot compute ordered distinct count over array of type \"{:?}\"", other);
            Err(DataFusionError::NotImplemented(message))
        }
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

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        for array in values {
            self.count += distinct_count(array)?;
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
