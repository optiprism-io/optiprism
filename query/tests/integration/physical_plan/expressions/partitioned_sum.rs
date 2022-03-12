#[cfg(test)]
mod tests {
    use query::error::Result;
    use arrow::array::{ArrayRef, Int8Array};
    use arrow::datatypes::DataType;
    use datafusion::scalar::ScalarValue as DFScalarValue;
    use std::sync::Arc;
    use query::physical_plan::expressions::aggregate::{AccumulatorEnum, AggregateFunction};
    use query::physical_plan::expressions::partitioned_aggregate::{PartitionedAggregate, PartitionedAggregateFunction};

    #[test]
    fn test() -> Result<()> {
        let spans = Arc::new(Int8Array::from(vec![1, 1, 2, 2, 2, 3, 3, 3, 3, 4]));
        let arr = Arc::new(Int8Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]));
        let agg = PartitionedAggregate::try_new(DataType::Int8, DataType::Int64, PartitionedAggregateFunction::Sum, DataType::Int64, AggregateFunction::Sum)?;
        let mut acc = agg.create_accumulator()?;
        acc.update_batch(&[spans.clone() as ArrayRef, arr.clone() as ArrayRef]);
        acc.update_batch(&[spans.clone() as ArrayRef, arr.clone() as ArrayRef]);
        assert_eq!(acc.state()?, vec![DFScalarValue::Int64(Some(100))]);
        Ok(())
    }
}