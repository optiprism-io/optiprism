use arrow::datatypes::DataType;
use datafusion_expr::AggregateFunction;

use crate::error::Result;

pub fn state_types(data_type: DataType, agg: &AggregateFunction) -> Result<Vec<DataType>> {
    Ok(match agg {
        AggregateFunction::Count => vec![DataType::UInt64],
        AggregateFunction::Sum => vec![data_type],
        AggregateFunction::Avg => vec![DataType::UInt64, data_type],
        AggregateFunction::Min => vec![data_type],
        AggregateFunction::Max => vec![data_type],
        _ => unimplemented!("{}", agg),
    })
}
