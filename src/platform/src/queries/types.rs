use chrono::DateTime;
use chrono::Utc;
use common::ScalarValue;
use common::DECIMAL_PRECISION;
use convert_case::Case;
use convert_case::Casing;
use num_traits::ToPrimitive;
use query::physical_plan::expressions::partitioned_aggregate::PartitionedAggregateFunction as QueryPartitionedAggregateFunction;
use query::queries::types as query_types;
use rust_decimal::Decimal;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Number;
use serde_json::Value;

use crate::PlatformError;
use crate::Result;

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
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
    Like,
    NotLike,
    NotRegex,
}

impl TryInto<common::types::PropValueOperation> for PropValueOperation {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<common::types::PropValueOperation, Self::Error> {
        Ok(match self {
            PropValueOperation::Eq => common::types::PropValueOperation::Eq,
            PropValueOperation::Neq => common::types::PropValueOperation::Neq,
            PropValueOperation::Gt => common::types::PropValueOperation::Gt,
            PropValueOperation::Gte => common::types::PropValueOperation::Gte,
            PropValueOperation::Lt => common::types::PropValueOperation::Lt,
            PropValueOperation::Lte => common::types::PropValueOperation::Lte,
            PropValueOperation::True => common::types::PropValueOperation::True,
            PropValueOperation::False => common::types::PropValueOperation::False,
            PropValueOperation::Exists => common::types::PropValueOperation::Exists,
            PropValueOperation::Empty => common::types::PropValueOperation::Empty,
            PropValueOperation::ArrAll => common::types::PropValueOperation::ArrAll,
            PropValueOperation::ArrAny => common::types::PropValueOperation::ArrAny,
            PropValueOperation::ArrNone => common::types::PropValueOperation::ArrNone,
            PropValueOperation::Regex => common::types::PropValueOperation::Regex,
            PropValueOperation::Like => common::types::PropValueOperation::Like,
            PropValueOperation::NotLike => common::types::PropValueOperation::NotLike,
            PropValueOperation::NotRegex => common::types::PropValueOperation::NotRegex,
        })
    }
}

impl TryInto<PropValueOperation> for common::types::PropValueOperation {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<PropValueOperation, Self::Error> {
        Ok(match self {
            common::types::PropValueOperation::Eq => PropValueOperation::Eq,
            common::types::PropValueOperation::Neq => PropValueOperation::Neq,
            common::types::PropValueOperation::Gt => PropValueOperation::Gt,
            common::types::PropValueOperation::Gte => PropValueOperation::Gte,
            common::types::PropValueOperation::Lt => PropValueOperation::Lt,
            common::types::PropValueOperation::Lte => PropValueOperation::Lte,
            common::types::PropValueOperation::True => PropValueOperation::True,
            common::types::PropValueOperation::False => PropValueOperation::False,
            common::types::PropValueOperation::Exists => PropValueOperation::Exists,
            common::types::PropValueOperation::Empty => PropValueOperation::Empty,
            common::types::PropValueOperation::ArrAll => PropValueOperation::ArrAll,
            common::types::PropValueOperation::ArrAny => PropValueOperation::ArrAny,
            common::types::PropValueOperation::ArrNone => PropValueOperation::ArrNone,
            common::types::PropValueOperation::Regex => PropValueOperation::Regex,
            common::types::PropValueOperation::Like => PropValueOperation::Like,
            common::types::PropValueOperation::NotLike => PropValueOperation::NotLike,
            common::types::PropValueOperation::NotRegex => PropValueOperation::NotRegex,
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
    type Error = PlatformError;

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
    type Error = PlatformError;

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
    type Error = PlatformError;

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
    type Error = PlatformError;

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
            let dec = Decimal::try_from(n.as_f64().unwrap())?;
            Ok(ScalarValue::Decimal128(
                Some(dec.mantissa()),
                DECIMAL_PRECISION,
                dec.scale() as usize,
            ))
        }
        Value::String(v) => Ok(ScalarValue::Utf8(Some(v.to_string()))),
        _ => Err(PlatformError::BadRequest("unexpected value".to_string())),
    }
}

pub fn scalar_to_json_value(v: &ScalarValue) -> Result<Value> {
    match v {
        ScalarValue::Decimal128(None, _, _) => Ok(Value::Null),
        ScalarValue::Boolean(None) => Ok(Value::Null),
        ScalarValue::Utf8(None) => Ok(Value::Null),
        ScalarValue::Decimal128(Some(v), _p, s) => Ok(Value::Number(
            Number::from_f64(Decimal::new(*v as i64, *s as u32).to_f64().unwrap()).unwrap(),
        )),
        ScalarValue::Boolean(Some(v)) => Ok(Value::Bool(*v)),
        ScalarValue::Utf8(Some(v)) => Ok(Value::String(v.to_owned())),
        _ => Err(PlatformError::BadRequest("unexpected value".to_string())),
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(tag = "eventType", rename_all = "camelCase")]
pub enum EventRef {
    #[serde(rename_all = "camelCase")]
    Regular { event_name: String },
    #[serde(rename_all = "camelCase")]
    Custom { event_id: u64 },
}

impl EventRef {
    pub fn name(&self, idx: usize) -> String {
        match self {
            EventRef::Regular { event_name } => {
                format!("{}_regular_{}", event_name.to_case(Case::Snake), idx)
            }
            EventRef::Custom { event_id } => format!("{}_custom_{}", event_id, idx),
        }
    }
}

impl From<EventRef> for common::types::EventRef {
    fn from(e: EventRef) -> Self {
        match e {
            EventRef::Regular { event_name } => common::types::EventRef::RegularName(event_name),
            EventRef::Custom { event_id } => common::types::EventRef::Custom(event_id),
        }
    }
}

impl TryInto<EventRef> for common::types::EventRef {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<EventRef, Self::Error> {
        Ok(match self {
            common::types::EventRef::RegularName(name) => EventRef::Regular { event_name: name },
            common::types::EventRef::Regular(_) => unimplemented!(),
            common::types::EventRef::Custom(id) => EventRef::Custom { event_id: id },
        })
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(tag = "propertyType", rename_all = "camelCase")]
pub enum PropertyRef {
    #[serde(rename_all = "camelCase")]
    User { property_name: String },
    #[serde(rename_all = "camelCase")]
    Event { property_name: String },
    #[serde(rename_all = "camelCase")]
    Custom { property_id: u64 },
}

impl TryInto<common::types::PropertyRef> for PropertyRef {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<common::types::PropertyRef, Self::Error> {
        Ok(match self {
            PropertyRef::User { property_name } => common::types::PropertyRef::User(property_name),
            PropertyRef::Event { property_name } => {
                common::types::PropertyRef::Event(property_name)
            }
            PropertyRef::Custom { property_id } => common::types::PropertyRef::Custom(property_id),
        })
    }
}

impl TryInto<PropertyRef> for common::types::PropertyRef {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<PropertyRef, Self::Error> {
        Ok(match self {
            common::types::PropertyRef::User(property_name) => PropertyRef::User { property_name },
            common::types::PropertyRef::Event(property_name) => {
                PropertyRef::Event { property_name }
            }
            common::types::PropertyRef::Custom(property_id) => PropertyRef::Custom { property_id },
        })
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum EventFilter {
    #[serde(rename_all = "camelCase")]
    Property {
        #[serde(flatten)]
        property: PropertyRef,
        operation: PropValueOperation,
        #[serde(skip_serializing_if = "Option::is_none")]
        value: Option<Vec<Value>>,
    },
}

impl TryInto<common::types::EventFilter> for &EventFilter {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<common::types::EventFilter, Self::Error> {
        Ok(match self {
            EventFilter::Property {
                property,
                operation,
                value,
            } => common::types::EventFilter::Property {
                property: property.to_owned().try_into()?,
                operation: operation.to_owned().try_into()?,
                value: match value {
                    None => None,
                    Some(v) => Some(v.iter().map(json_value_to_scalar).collect::<Result<_>>()?),
                },
            },
        })
    }
}

impl TryInto<EventFilter> for common::types::EventFilter {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<EventFilter, Self::Error> {
        Ok(match self {
            common::types::EventFilter::Property {
                property,
                operation,
                value,
            } => EventFilter::Property {
                property: property.try_into()?,
                operation: operation.try_into()?,
                value: match value {
                    None => None,
                    Some(v) => Some(v.iter().map(scalar_to_json_value).collect::<Result<_>>()?),
                },
            },
        })
    }
}
