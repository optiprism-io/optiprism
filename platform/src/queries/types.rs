use crate::Error;
use crate::Result;
use chrono::{DateTime, Utc};
use common::DECIMAL_PRECISION;
use convert_case::{Case, Casing};
use datafusion::scalar::ScalarValue;
use query::physical_plan::expressions::partitioned_aggregate::PartitionedAggregateFunction as QueryPartitionedAggregateFunction;
use query::queries::event_segmentation::types as query_es_types;
use query::queries::event_segmentation::types::NamedQuery;
use query::queries::types as query_types;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use crate::queries::event_segmentation::PropertyType;

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PropValueOperation {
    Eq,
    Neq,
    Gt,
    Gte,
    Lt,
    Lte,
    True,
    False,
    Exists,
    Empty,
    ArrAll,
    ArrAny,
    ArrNone,
    Regex,
}

impl TryInto<query_types::PropValueOperation> for PropValueOperation {
    type Error = Error;

    fn try_into(self) -> std::result::Result<query_types::PropValueOperation, Self::Error> {
        Ok(match self {
            PropValueOperation::Eq => query_types::PropValueOperation::Eq,
            PropValueOperation::Neq => query_types::PropValueOperation::Neq,
            PropValueOperation::Gt => query_types::PropValueOperation::Gt,
            PropValueOperation::Gte => query_types::PropValueOperation::Gte,
            PropValueOperation::Lt => query_types::PropValueOperation::Lt,
            PropValueOperation::Lte => query_types::PropValueOperation::Lte,
            PropValueOperation::True => query_types::PropValueOperation::True,
            PropValueOperation::False => query_types::PropValueOperation::False,
            PropValueOperation::Exists => query_types::PropValueOperation::Exists,
            PropValueOperation::Empty => query_types::PropValueOperation::Empty,
            PropValueOperation::ArrAll => query_types::PropValueOperation::ArrAll,
            PropValueOperation::ArrAny => query_types::PropValueOperation::ArrAny,
            PropValueOperation::ArrNone => query_types::PropValueOperation::ArrNone,
            PropValueOperation::Regex => query_types::PropValueOperation::Regex,
        })
    }
}


#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum QueryTime {
    Between {
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    },
    From(DateTime<Utc>),
    Last {
        last: i64,
        unit: TimeUnit,
    },
}

impl TryInto<query_types::QueryTime> for QueryTime {
    type Error = Error;

    fn try_into(self) -> std::result::Result<query_types::QueryTime, Self::Error> {
        Ok(match self {
            QueryTime::Between { from, to } => query_types::QueryTime::Between { from, to },
            QueryTime::From(v) => query_types::QueryTime::From(v),
            QueryTime::Last { last, unit } => query_types::QueryTime::Last {
                last,
                unit: unit.try_into()?,
            },
        })
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TimeUnit {
    Second,
    Minute,
    Hour,
    Day,
    Week,
    Month,
    Year,
}

impl TryInto<query_types::TimeUnit> for TimeUnit {
    type Error = Error;

    fn try_into(self) -> std::result::Result<query_types::TimeUnit, Self::Error> {
        Ok(match self {
            TimeUnit::Second => query_types::TimeUnit::Second,
            TimeUnit::Minute => query_types::TimeUnit::Minute,
            TimeUnit::Hour => query_types::TimeUnit::Hour,
            TimeUnit::Day => query_types::TimeUnit::Day,
            TimeUnit::Week => query_types::TimeUnit::Week,
            TimeUnit::Month => query_types::TimeUnit::Month,
            TimeUnit::Year => query_types::TimeUnit::Year,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
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
}

impl TryInto<datafusion::physical_plan::aggregates::AggregateFunction> for &AggregateFunction {
    type Error = Error;

    fn try_into(
        self,
    ) -> std::result::Result<datafusion::physical_plan::aggregates::AggregateFunction, Self::Error>
    {
        Ok(match self {
            AggregateFunction::Count => {
                datafusion::physical_plan::aggregates::AggregateFunction::Count
            }
            AggregateFunction::Sum => datafusion::physical_plan::aggregates::AggregateFunction::Sum,
            AggregateFunction::Min => datafusion::physical_plan::aggregates::AggregateFunction::Min,
            AggregateFunction::Max => datafusion::physical_plan::aggregates::AggregateFunction::Max,
            AggregateFunction::Avg => datafusion::physical_plan::aggregates::AggregateFunction::Avg,
            AggregateFunction::ApproxDistinct => {
                datafusion::physical_plan::aggregates::AggregateFunction::ApproxDistinct
            }
            AggregateFunction::ArrayAgg => {
                datafusion::physical_plan::aggregates::AggregateFunction::ArrayAgg
            }
            AggregateFunction::Variance => {
                datafusion::physical_plan::aggregates::AggregateFunction::Variance
            }
            AggregateFunction::VariancePop => {
                datafusion::physical_plan::aggregates::AggregateFunction::VariancePop
            }
            AggregateFunction::Stddev => {
                datafusion::physical_plan::aggregates::AggregateFunction::Stddev
            }
            AggregateFunction::StddevPop => {
                datafusion::physical_plan::aggregates::AggregateFunction::StddevPop
            }
            AggregateFunction::Covariance => {
                datafusion::physical_plan::aggregates::AggregateFunction::Covariance
            }
            AggregateFunction::CovariancePop => {
                datafusion::physical_plan::aggregates::AggregateFunction::CovariancePop
            }
            AggregateFunction::Correlation => {
                datafusion::physical_plan::aggregates::AggregateFunction::Correlation
            }
            AggregateFunction::ApproxPercentileCont => {
                datafusion::physical_plan::aggregates::AggregateFunction::ApproxPercentileCont
            }
            AggregateFunction::ApproxMedian => {
                datafusion::physical_plan::aggregates::AggregateFunction::ApproxMedian
            }
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PartitionedAggregateFunction {
    Count,
    Sum,
}

impl TryInto<QueryPartitionedAggregateFunction> for &PartitionedAggregateFunction {
    type Error = Error;

    fn try_into(self) -> std::result::Result<QueryPartitionedAggregateFunction, Self::Error> {
        Ok(match self {
            PartitionedAggregateFunction::Count => QueryPartitionedAggregateFunction::Count,
            PartitionedAggregateFunction::Sum => QueryPartitionedAggregateFunction::Sum,
        })
    }
}

pub fn json_value_to_scalar(v: &Value) -> Result<ScalarValue> {
    match v {
        Value::Bool(v) => Ok(ScalarValue::Boolean(Some(*v))),
        Value::Number(n) => {
            let dec = Decimal::try_from(n.as_f64().unwrap())
                .map_err(|e| Error::BadRequest(e.to_string()))?;
            Ok(ScalarValue::Decimal128(
                Some(dec.mantissa()),
                DECIMAL_PRECISION,
                dec.scale() as usize,
            ))
        }
        Value::String(v) => Ok(ScalarValue::Utf8(Some(v.to_string()))),
        _ => Err(Error::BadRequest("unexpected value".to_string())),
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "eventType", rename_all = "camelCase")]
pub enum EventRef {
    Regular { event_name: String },
    Custom { event_id: u64 },
}

impl EventRef {
    pub fn name(&self, idx:usize) -> String {
        match self {
            EventRef::Regular { event_name } => format!("{}_regular_{}",event_name.to_case(Case::Snake),idx),
            EventRef::Custom { event_id } => format!("{}_custom_{}",event_id,idx),
        }
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "propertyType", rename_all = "camelCase")]
pub enum PropertyRef {
    User { property_name: String },
    Event { property_name: String },
    Custom { property_id: u64 },
}

impl TryInto<query_types::EventRef> for EventRef {
    type Error = Error;

    fn try_into(self) -> std::result::Result<query_types::EventRef, Self::Error> {
        Ok(match self {
            EventRef::Regular { event_name } => query_types::EventRef::Regular(event_name),
            EventRef::Custom { event_id } => query_types::EventRef::Custom(event_id),
        })
    }
}

impl TryInto<query_types::PropertyRef> for PropertyRef {
    type Error = Error;

    fn try_into(self) -> std::result::Result<query_types::PropertyRef, Self::Error> {
        Ok(match self {
            PropertyRef::User { property_name } => query_types::PropertyRef::User(property_name),
            PropertyRef::Event { property_name } => query_types::PropertyRef::Event(property_name),
            PropertyRef::Custom { property_id } => query_types::PropertyRef::Custom(property_id),
        })
    }
}