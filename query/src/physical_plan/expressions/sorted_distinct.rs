use std::cmp::Ordering;
use std::fmt::Debug;
use std::sync::Arc;
use arrow::array::{Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Date64Array, DecimalArray, DictionaryArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, LargeBinaryArray, LargeStringArray, ListArray, PrimitiveArray, StringArray, StructArray, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array};
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

macro_rules! distinct_count_array {
    ($VALUES:expr, $ARRAYTYPE:ident) => {{
        let array = $VALUES.as_any().downcast_ref::<$ARRAYTYPE>().unwrap();
        let mut current = None;
        let mut count: u64 = 0;
        for i in 0..array.len() {
            if array.is_null(i) {
                continue;
            }
            let val = array.value(i);
            if current.is_none() || !(&val).eq(current.as_ref().unwrap()) {
                current = Some(val);
                count += 1;
            }
        }
        Ok(count)
    }};
}

macro_rules! distinct_count_array_limited {
    ($VALUES:expr, $ARRAYTYPE:ident, $LIMIT:expr) => {{
        let array = $VALUES.as_any().downcast_ref::<$ARRAYTYPE>().unwrap();
        let mut current = None;
        let mut count: u64 = 0;
        for i in 0..array.len() {
            if array.is_null(i) {
                continue;
            }
            let val = array.value(i);
            if current.is_none() || !(&val).eq(current.as_ref().unwrap()) {
                current = Some(val);
                count += 1;
                if count >= $LIMIT {
                    return Ok(count);
                }
            }
        }
        Ok(count)
    }};
}

fn distinct_count(array: &ArrayRef) -> Result<u64> {
    match array.data_type() {
        DataType::Boolean => distinct_count_array_limited!(array, BooleanArray, 2),
        DataType::Utf8 => distinct_count_array!(array, StringArray),
        DataType::Decimal(_, _) => distinct_count_array!(array, DecimalArray),
        DataType::UInt64 => distinct_count_array!(array, UInt64Array),
        DataType::UInt32 => distinct_count_array!(array, UInt32Array),
        DataType::UInt16 => distinct_count_array!(array, UInt16Array),
        DataType::UInt8 => distinct_count_array!(array, UInt8Array),
        DataType::Int64 => distinct_count_array!(array, Int64Array),
        DataType::Int32 => distinct_count_array!(array, Int32Array),
        DataType::Int16 => distinct_count_array!(array, Int16Array),
        DataType::Int8 => distinct_count_array!(array, Int8Array),
        DataType::Date32 => distinct_count_array!(array, Date32Array),
        DataType::Date64 => distinct_count_array!(array, Date64Array),
        DataType::Timestamp(TimeUnit::Second, _) => distinct_count_array!(array, TimestampSecondArray),
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
        let value = &values[0];
        if value.is_null() {
            return Ok(());
        }
        if self.current.is_null() || !self.current.eq(value) {
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
