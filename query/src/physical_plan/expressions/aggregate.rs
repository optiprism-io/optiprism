use std::fmt;
use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use datafusion_expr::Accumulator;
use crate::error::{Error, Result};
use crate::physical_plan::expressions::sum::SumAccumulator;
use datafusion_expr::AggregateFunction as DFAggregateFunction;
use datafusion::scalar::ScalarValue as DFScalarValue;
use datafusion::error::Result as DFResult;

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
    // SortedDistinctCount
    SortedDistinctCount,
}

// enum storage for accumulator for fast static dispatching and easy translating between threads
#[derive(Debug, Clone)]
pub enum AccumulatorEnum {
    Sum(SumAccumulator),
}

impl Accumulator for AccumulatorEnum {
    fn state(&self) -> DFResult<Vec<DFScalarValue>> {
        match self {
            AccumulatorEnum::Sum(acc) => acc.state(),
        }
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        match self {
            AccumulatorEnum::Sum(acc) => acc.update_batch(values),
        }
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        match self {
            AccumulatorEnum::Sum(acc) => acc.merge_batch(states),
        }
    }

    fn evaluate(&self) -> DFResult<DFScalarValue> {
        match self {
            AccumulatorEnum::Sum(acc) => acc.evaluate(),
        }
    }
}

impl fmt::Display for AggregateFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", format!("{:?}", self).to_uppercase())
    }
}


impl TryInto<DFAggregateFunction> for AggregateFunction {
    type Error = crate::Error;

    fn try_into(self) -> std::result::Result<DFAggregateFunction, Self::Error> {
        Ok(match self {
            AggregateFunction::Count => DFAggregateFunction::Count,
            AggregateFunction::Sum => DFAggregateFunction::Sum,
            AggregateFunction::Min => DFAggregateFunction::Min,
            AggregateFunction::Max => DFAggregateFunction::Max,
            AggregateFunction::Avg => DFAggregateFunction::Avg,
            AggregateFunction::ApproxDistinct => DFAggregateFunction::ApproxDistinct,
            AggregateFunction::ArrayAgg => DFAggregateFunction::ArrayAgg,
            AggregateFunction::Variance => DFAggregateFunction::Variance,
            AggregateFunction::VariancePop => DFAggregateFunction::VariancePop,
            AggregateFunction::Stddev => DFAggregateFunction::Stddev,
            AggregateFunction::StddevPop => DFAggregateFunction::StddevPop,
            AggregateFunction::Covariance => DFAggregateFunction::Covariance,
            AggregateFunction::CovariancePop => DFAggregateFunction::CovariancePop,
            AggregateFunction::Correlation => DFAggregateFunction::Correlation,
            AggregateFunction::ApproxPercentileCont => DFAggregateFunction::ApproxPercentileCont,
            AggregateFunction::ApproxMedian => DFAggregateFunction::ApproxMedian,
            AggregateFunction::SortedDistinctCount => unreachable!()
        })
    }
}

impl TryInto<DFAggregateFunction> for &AggregateFunction {
    type Error = crate::Error;

    fn try_into(self) -> std::result::Result<DFAggregateFunction, Self::Error> {
        Ok(match self {
            AggregateFunction::Count => DFAggregateFunction::Count,
            AggregateFunction::Sum => DFAggregateFunction::Sum,
            AggregateFunction::Min => DFAggregateFunction::Min,
            AggregateFunction::Max => DFAggregateFunction::Max,
            AggregateFunction::Avg => DFAggregateFunction::Avg,
            AggregateFunction::ApproxDistinct => DFAggregateFunction::ApproxDistinct,
            AggregateFunction::ArrayAgg => DFAggregateFunction::ArrayAgg,
            AggregateFunction::Variance => DFAggregateFunction::Variance,
            AggregateFunction::VariancePop => DFAggregateFunction::VariancePop,
            AggregateFunction::Stddev => DFAggregateFunction::Stddev,
            AggregateFunction::StddevPop => DFAggregateFunction::StddevPop,
            AggregateFunction::Covariance => DFAggregateFunction::Covariance,
            AggregateFunction::CovariancePop => DFAggregateFunction::CovariancePop,
            AggregateFunction::Correlation => DFAggregateFunction::Correlation,
            AggregateFunction::ApproxPercentileCont => DFAggregateFunction::ApproxPercentileCont,
            AggregateFunction::ApproxMedian => DFAggregateFunction::ApproxMedian,
            AggregateFunction::SortedDistinctCount => unreachable!()
        })
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

pub fn new_accumulator(
    agg: &AggregateFunction,
    data_type: &DataType,
) -> Result<AccumulatorEnum> {
    Ok(match agg {
        AggregateFunction::Sum => {
            AccumulatorEnum::Sum(SumAccumulator::try_new(data_type)?)
        }
        _ => unimplemented!(),
    })
}