use std::fmt;
use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use datafusion_expr::Accumulator;
use crate::error::{Error, Result};
use datafusion_expr::AggregateFunction as DFAggregateFunction;
use datafusion::scalar::ScalarValue as DFScalarValue;
use datafusion::error::Result as DFResult;
use datafusion::physical_plan::aggregates;
use datafusion::physical_plan::expressions::{AvgAccumulator, MaxAccumulator, MinAccumulator};

pub fn return_type(
    fun: &AggregateFunction,
    input_expr_types: &[DataType],
) -> Result<DataType> {
    Ok(match fun {
        AggregateFunction::SortedDistinctCount => DataType::UInt64,
        _ => aggregates::return_type(&fun.try_into()?, input_expr_types)?,
    })
}


/// Enum of all built-in aggregate functions
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum AggregateFunction {
    /// count
    Count,
    /// sum
    Sum,
    /// min
    Min,
    /// max
    Max,
    /// avg
    Avg,
    /// Approximate aggregate function
    ApproxDistinct,
    /// array_agg
    ArrayAgg,
    /// Variance (Sample)
    Variance,
    /// Variance (Population)
    VariancePop,
    /// Standard Deviation (Sample)
    Stddev,
    /// Standard Deviation (Population)
    StddevPop,
    /// Covariance (Sample)
    Covariance,
    /// Covariance (Population)
    CovariancePop,
    /// Correlation
    Correlation,
    /// Approximate continuous percentile function
    ApproxPercentileCont,
    /// ApproxMedian
    ApproxMedian,
    /// SortedDistinctCount
    SortedDistinctCount,
}

/*// enum storage for accumulator for fast static dispatching and easy translating between threads
#[derive(Debug)]
pub enum AccumulatorEnum {
    Sum(SumAccumulator),
    Count(CountAccumulator),
    Avg(AvgAccumulator),
    Min(MinAccumulator),
    Max(MaxAccumulator),
}

impl Clone for AccumulatorEnum {
    fn clone(&self) -> Self {
        match self {
            AccumulatorEnum::Sum(acc) => acc.
            AccumulatorEnum::Count(acc) => {}
            AccumulatorEnum::Avg(acc) => {}
            AccumulatorEnum::Min(acc) => {}
            AccumulatorEnum::Max(acc) => {}
        }
    }
}
impl Accumulator for AccumulatorEnum {
    fn state(&self) -> DFResult<Vec<DFScalarValue>> {
        match self {
            AccumulatorEnum::Sum(acc) => acc.state(),
            AccumulatorEnum::Count(acc) => acc.state(),
            AccumulatorEnum::Avg(acc) => acc.state(),
            AccumulatorEnum::Min(acc) => acc.state(),
            AccumulatorEnum::Max(acc) => acc.state(),
        }
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        match self {
            AccumulatorEnum::Sum(acc) => acc.update_batch(values),
            AccumulatorEnum::Count(acc) => acc.update_batch(values),
            AccumulatorEnum::Avg(acc) => acc.update_batch(values),
            AccumulatorEnum::Min(acc) => acc.update_batch(values),
            AccumulatorEnum::Max(acc) => acc.update_batch(values),
        }
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        match self {
            AccumulatorEnum::Sum(acc) => acc.merge_batch(states),
            AccumulatorEnum::Count(acc) => acc.merge_batch(states),
            AccumulatorEnum::Avg(acc) => acc.merge_batch(states),
            AccumulatorEnum::Min(acc) => acc.merge_batch(states),
            AccumulatorEnum::Max(acc) => acc.merge_batch(states),
        }
    }

    fn evaluate(&self) -> DFResult<DFScalarValue> {
        match self {
            AccumulatorEnum::Sum(acc) => acc.evaluate(),
            AccumulatorEnum::Count(acc) => acc.evaluate(),
            AccumulatorEnum::Avg(acc) => acc.evaluate(),
            AccumulatorEnum::Min(acc) => acc.evaluate(),
            AccumulatorEnum::Max(acc) => acc.evaluate(),
        }
    }
}
*/
impl fmt::Display for AggregateFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", format!("{:?}", self).to_uppercase())
    }
}


impl TryInto<DFAggregateFunction> for AggregateFunction {
    type Error = crate::Error;

    fn try_into(self) -> std::result::Result<DFAggregateFunction, Self::Error> {
        match self {
            AggregateFunction::Count => Ok(DFAggregateFunction::Count),
            AggregateFunction::Sum => Ok(DFAggregateFunction::Sum),
            AggregateFunction::Min => Ok(DFAggregateFunction::Min),
            AggregateFunction::Max => Ok(DFAggregateFunction::Max),
            AggregateFunction::Avg => Ok(DFAggregateFunction::Avg),
            AggregateFunction::ApproxDistinct => Ok(DFAggregateFunction::ApproxDistinct),
            AggregateFunction::ArrayAgg => Ok(DFAggregateFunction::ArrayAgg),
            AggregateFunction::Variance => Ok(DFAggregateFunction::Variance),
            AggregateFunction::VariancePop => Ok(DFAggregateFunction::VariancePop),
            AggregateFunction::Stddev => Ok(DFAggregateFunction::Stddev),
            AggregateFunction::StddevPop => Ok(DFAggregateFunction::StddevPop),
            AggregateFunction::Covariance => Ok(DFAggregateFunction::Covariance),
            AggregateFunction::CovariancePop => Ok(DFAggregateFunction::CovariancePop),
            AggregateFunction::Correlation => Ok(DFAggregateFunction::Correlation),
            AggregateFunction::ApproxPercentileCont => Ok(DFAggregateFunction::ApproxPercentileCont),
            AggregateFunction::ApproxMedian => Ok(DFAggregateFunction::ApproxMedian),
            AggregateFunction::SortedDistinctCount => Err(Error::Internal("can't convert".to_string())),
        }
    }
}

impl TryInto<DFAggregateFunction> for &AggregateFunction {
    type Error = crate::Error;

    fn try_into(self) -> std::result::Result<DFAggregateFunction, Self::Error> {
        match self {
            AggregateFunction::Count => Ok(DFAggregateFunction::Count),
            AggregateFunction::Sum => Ok(DFAggregateFunction::Sum),
            AggregateFunction::Min => Ok(DFAggregateFunction::Min),
            AggregateFunction::Max => Ok(DFAggregateFunction::Max),
            AggregateFunction::Avg => Ok(DFAggregateFunction::Avg),
            AggregateFunction::ApproxDistinct => Ok(DFAggregateFunction::ApproxDistinct),
            AggregateFunction::ArrayAgg => Ok(DFAggregateFunction::ArrayAgg),
            AggregateFunction::Variance => Ok(DFAggregateFunction::Variance),
            AggregateFunction::VariancePop => Ok(DFAggregateFunction::VariancePop),
            AggregateFunction::Stddev => Ok(DFAggregateFunction::Stddev),
            AggregateFunction::StddevPop => Ok(DFAggregateFunction::StddevPop),
            AggregateFunction::Covariance => Ok(DFAggregateFunction::Covariance),
            AggregateFunction::CovariancePop => Ok(DFAggregateFunction::CovariancePop),
            AggregateFunction::Correlation => Ok(DFAggregateFunction::Correlation),
            AggregateFunction::ApproxPercentileCont => Ok(DFAggregateFunction::ApproxPercentileCont),
            AggregateFunction::ApproxMedian => Ok(DFAggregateFunction::ApproxMedian),
            AggregateFunction::SortedDistinctCount => Err(Error::Internal("can't convert".to_string())),
        }
    }
}

impl From<DFAggregateFunction> for AggregateFunction {
    fn from(value: DFAggregateFunction) -> Self {
        match value {
            DFAggregateFunction::Count => AggregateFunction::Count,
            DFAggregateFunction::Sum => AggregateFunction::Sum,
            DFAggregateFunction::Min => AggregateFunction::Min,
            DFAggregateFunction::Max => AggregateFunction::Max,
            DFAggregateFunction::Avg => AggregateFunction::Avg,
            DFAggregateFunction::ApproxDistinct => AggregateFunction::ApproxDistinct,
            DFAggregateFunction::ArrayAgg => AggregateFunction::ArrayAgg,
            DFAggregateFunction::Variance => AggregateFunction::Variance,
            DFAggregateFunction::VariancePop => AggregateFunction::VariancePop,
            DFAggregateFunction::Stddev => AggregateFunction::Stddev,
            DFAggregateFunction::StddevPop => AggregateFunction::StddevPop,
            DFAggregateFunction::Covariance => AggregateFunction::Covariance,
            DFAggregateFunction::CovariancePop => AggregateFunction::CovariancePop,
            DFAggregateFunction::Correlation => AggregateFunction::Correlation,
            DFAggregateFunction::ApproxPercentileCont => AggregateFunction::ApproxPercentileCont,
            DFAggregateFunction::ApproxMedian => AggregateFunction::ApproxMedian,
        }
    }
}

impl TryFrom<&DFAggregateFunction> for AggregateFunction {
    type Error = crate::Error;

    fn try_from(value: &DFAggregateFunction) -> std::result::Result<Self, Self::Error> {
        Ok(match value {
            DFAggregateFunction::Count => AggregateFunction::Count,
            DFAggregateFunction::Sum => AggregateFunction::Sum,
            DFAggregateFunction::Min => AggregateFunction::Min,
            DFAggregateFunction::Max => AggregateFunction::Max,
            DFAggregateFunction::Avg => AggregateFunction::Avg,
            DFAggregateFunction::ApproxDistinct => AggregateFunction::ApproxDistinct,
            DFAggregateFunction::ArrayAgg => AggregateFunction::ArrayAgg,
            DFAggregateFunction::Variance => AggregateFunction::Variance,
            DFAggregateFunction::VariancePop => AggregateFunction::VariancePop,
            DFAggregateFunction::Stddev => AggregateFunction::Stddev,
            DFAggregateFunction::StddevPop => AggregateFunction::StddevPop,
            DFAggregateFunction::Covariance => AggregateFunction::Covariance,
            DFAggregateFunction::CovariancePop => AggregateFunction::CovariancePop,
            DFAggregateFunction::Correlation => AggregateFunction::Correlation,
            DFAggregateFunction::ApproxPercentileCont => AggregateFunction::ApproxPercentileCont,
            DFAggregateFunction::ApproxMedian => AggregateFunction::ApproxMedian,
        })
    }
}
/*
pub fn new_accumulator(
    agg: &AggregateFunction,
    data_type: &DataType,
) -> Result<AccumulatorEnum> {
    match agg {
        AggregateFunction::Sum => Ok(AccumulatorEnum::Sum(SumAccumulator::try_new(data_type)?)),
        AggregateFunction::Count => Ok(AccumulatorEnum::Count(CountAccumulator::new())),
        AggregateFunction::Min => Ok(AccumulatorEnum::Min(MinAccumulator::try_new(data_type)?)),
        AggregateFunction::Max => Ok(AccumulatorEnum::Max(MaxAccumulator::try_new(data_type)?)),
        AggregateFunction::Avg => Ok(AccumulatorEnum::Avg(AvgAccumulator::try_new(data_type)?)),
        _ => Err(Error::Internal(format!("{:?} doesn't supported", agg))),
    }
}*/