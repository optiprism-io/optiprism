use crate::error::{Error, Result};
use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion::physical_plan::aggregates;
use datafusion::physical_plan::expressions::AvgAccumulator;
use datafusion::scalar::ScalarValue as DFScalarValue;
use datafusion_expr::AggregateFunction as DFAggregateFunction;
use datafusion_expr::{Accumulator, AggregateFunction};
use std::fmt;

pub fn state_types(data_type: DataType, agg: &AggregateFunction) -> Result<Vec<DataType>> {
    Ok(match agg {
        AggregateFunction::Count => vec![DataType::UInt64],
        AggregateFunction::Sum => vec![data_type],
        AggregateFunction::Avg => vec![DataType::UInt64, data_type],
        _ => unimplemented!(),
    })
}
