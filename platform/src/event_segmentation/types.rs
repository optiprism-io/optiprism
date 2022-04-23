use std::fmt::format;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use datafusion::scalar::ScalarValue;
use query::reports::event_segmentation::types as query_es_types;
use query::reports::types as query_types;
use crate::Error;
use query::physical_plan::expressions::partitioned_aggregate::PartitionedAggregateFunction as QueryPartitionedAggregateFunction;
use crate::Result;
use rust_decimal::Decimal;
use common::DECIMAL_PRECISION;
use query::reports::event_segmentation::types::NamedQuery;
use convert_case::{Case, Casing};

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
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
            QueryTime::Last { last, unit } => query_types::QueryTime::Last { last, unit: unit.try_into()? }
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

impl TryInto<query_types::PropValueOperation> for &PropValueOperation {
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

    fn try_into(self) -> std::result::Result<datafusion::physical_plan::aggregates::AggregateFunction, Self::Error> {
        Ok(match self {
            AggregateFunction::Count => datafusion::physical_plan::aggregates::AggregateFunction::Count,
            AggregateFunction::Sum => datafusion::physical_plan::aggregates::AggregateFunction::Sum,
            AggregateFunction::Min => datafusion::physical_plan::aggregates::AggregateFunction::Min,
            AggregateFunction::Max => datafusion::physical_plan::aggregates::AggregateFunction::Max,
            AggregateFunction::Avg => datafusion::physical_plan::aggregates::AggregateFunction::Avg,
            AggregateFunction::ApproxDistinct => datafusion::physical_plan::aggregates::AggregateFunction::ApproxDistinct,
            AggregateFunction::ArrayAgg => datafusion::physical_plan::aggregates::AggregateFunction::ArrayAgg,
            AggregateFunction::Variance => datafusion::physical_plan::aggregates::AggregateFunction::Variance,
            AggregateFunction::VariancePop => datafusion::physical_plan::aggregates::AggregateFunction::VariancePop,
            AggregateFunction::Stddev => datafusion::physical_plan::aggregates::AggregateFunction::Stddev,
            AggregateFunction::StddevPop => datafusion::physical_plan::aggregates::AggregateFunction::StddevPop,
            AggregateFunction::Covariance => datafusion::physical_plan::aggregates::AggregateFunction::Covariance,
            AggregateFunction::CovariancePop => datafusion::physical_plan::aggregates::AggregateFunction::CovariancePop,
            AggregateFunction::Correlation => datafusion::physical_plan::aggregates::AggregateFunction::Correlation,
            AggregateFunction::ApproxPercentileCont => datafusion::physical_plan::aggregates::AggregateFunction::ApproxPercentileCont,
            AggregateFunction::ApproxMedian => datafusion::physical_plan::aggregates::AggregateFunction::ApproxMedian,
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

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum SegmentTime {
    Between {
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    },
    From(DateTime<Utc>),
    Last {
        n: i64,
        unit: TimeUnit,
    },
    AfterFirstUse {
        within: i64,
        unit: TimeUnit,
    },
    WindowEach {
        unit: TimeUnit,
    },
}

impl TryInto<query_es_types::SegmentTime> for SegmentTime {
    type Error = Error;

    fn try_into(self) -> std::result::Result<query_es_types::SegmentTime, Self::Error> {
        Ok(match self {
            SegmentTime::Between { from, to } => query_es_types::SegmentTime::Between { from, to },
            SegmentTime::From(v) => query_es_types::SegmentTime::From(v),
            SegmentTime::Last { n, unit } => query_es_types::SegmentTime::Last { n, unit: unit.try_into()? },
            SegmentTime::AfterFirstUse { within, unit } => query_es_types::SegmentTime::AfterFirstUse { within, unit: unit.try_into()? },
            SegmentTime::WindowEach { unit } => query_es_types::SegmentTime::WindowEach { unit: unit.try_into()? }
        })
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ChartType {
    Line,
    Bar,
}

impl TryInto<query_es_types::ChartType> for ChartType {
    type Error = Error;

    fn try_into(self) -> std::result::Result<query_es_types::ChartType, Self::Error> {
        Ok(match self {
            ChartType::Line => query_es_types::ChartType::Line,
            ChartType::Bar => query_es_types::ChartType::Bar,
        })
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Analysis {
    Linear,
    RollingAverage { window: usize, unit: TimeUnit },
    WindowAverage { window: usize, unit: TimeUnit },
    Cumulative,
}

impl TryInto<query_es_types::Analysis> for Analysis {
    type Error = Error;

    fn try_into(self) -> std::result::Result<query_es_types::Analysis, Self::Error> {
        Ok(match self {
            Analysis::Linear => query_es_types::Analysis::Linear,
            Analysis::RollingAverage { window, unit } => query_es_types::Analysis::RollingAverage { window, unit: unit.try_into()? },
            Analysis::WindowAverage { window, unit } => query_es_types::Analysis::WindowAverage { window, unit: unit.try_into()? },
            Analysis::Cumulative => query_es_types::Analysis::Cumulative
        })
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Compare {
    pub offset: usize,
    pub unit: TimeUnit,
}

/*impl TryFrom<Option<Compare>> for Option<query_es_types::Compare> {
    type Error = Error;

    fn try_from(value: Option<Compare>) -> std::result::Result<Self, Self::Error> {
        Ok(match value {
            None => None,
            Some(v) => v.try_into()?
        })
    }
}
*/
impl TryInto<query_es_types::Compare> for Compare {
    type Error = Error;

    fn try_into(self) -> std::result::Result<query_es_types::Compare, Self::Error> {
        Ok(query_es_types::Compare { offset: self.offset, unit: self.unit.try_into()? })
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum QueryAggregate {
    Min,
    Max,
    Sum,
    Avg,
    Median,
    DistinctCount,
    Percentile25th,
    Percentile75th,
    Percentile90th,
    Percentile99th,
}

impl TryInto<query_es_types::QueryAggregate> for QueryAggregate {
    type Error = Error;

    fn try_into(self) -> std::result::Result<query_es_types::QueryAggregate, Self::Error> {
        Ok(match self {
            QueryAggregate::Min => query_es_types::QueryAggregate::Min,
            QueryAggregate::Max => query_es_types::QueryAggregate::Max,
            QueryAggregate::Sum => query_es_types::QueryAggregate::Sum,
            QueryAggregate::Avg => query_es_types::QueryAggregate::Avg,
            QueryAggregate::Median => query_es_types::QueryAggregate::Median,
            QueryAggregate::DistinctCount => query_es_types::QueryAggregate::DistinctCount,
            QueryAggregate::Percentile25th => query_es_types::QueryAggregate::Percentile25th,
            QueryAggregate::Percentile75th => query_es_types::QueryAggregate::Percentile75th,
            QueryAggregate::Percentile90th => query_es_types::QueryAggregate::Percentile90th,
            QueryAggregate::Percentile99th => query_es_types::QueryAggregate::Percentile99th,
        })
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum QueryAggregatePerGroup {
    Min,
    Max,
    Sum,
    Avg,
    Median,
    DistinctCount,
}

impl TryInto<query_es_types::QueryAggregatePerGroup> for QueryAggregatePerGroup {
    type Error = Error;

    fn try_into(self) -> std::result::Result<query_es_types::QueryAggregatePerGroup, Self::Error> {
        Ok(match self {
            QueryAggregatePerGroup::Min => query_es_types::QueryAggregatePerGroup::Min,
            QueryAggregatePerGroup::Max => query_es_types::QueryAggregatePerGroup::Max,
            QueryAggregatePerGroup::Sum => query_es_types::QueryAggregatePerGroup::Sum,
            QueryAggregatePerGroup::Avg => query_es_types::QueryAggregatePerGroup::Avg,
            QueryAggregatePerGroup::Median => query_es_types::QueryAggregatePerGroup::Median,
            QueryAggregatePerGroup::DistinctCount => query_es_types::QueryAggregatePerGroup::DistinctCount,
        })
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum QueryPerGroup {
    CountEvents,
}

impl TryInto<query_es_types::QueryPerGroup> for QueryPerGroup {
    type Error = Error;

    fn try_into(self) -> std::result::Result<query_es_types::QueryPerGroup, Self::Error> {
        Ok(match self {
            QueryPerGroup::CountEvents => query_es_types::QueryPerGroup::CountEvents,
        })
    }
}

fn convert_property(prop_type: PropertyType, prop_name: String) -> query_types::PropertyRef {
    match prop_type {
        PropertyType::User => query_types::PropertyRef::User(prop_name),
        PropertyType::Event => query_types::PropertyRef::Event(prop_name),
        PropertyType::Custom => query_types::PropertyRef::Custom(prop_name),
    }
}

fn try_convert_value(v: &Value) -> Result<ScalarValue> {
    match v {
        Value::Bool(v) => Ok(ScalarValue::Boolean(Some(v.clone()))),
        Value::Number(n) => {
            let dec = Decimal::try_from(n.as_f64().unwrap()).map_err(|e| Error::BadRequest(e.to_string()))?;
            Ok(ScalarValue::Decimal128(Some(dec.mantissa()), DECIMAL_PRECISION, dec.scale() as usize))
        }
        Value::String(v) => Ok(ScalarValue::Utf8(Some(v.to_string()))),
        _ => Err(Error::BadRequest("unexpected value".to_string())),
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum Query {
    CountEvents,
    CountUniqueGroups,
    DailyActiveGroups,
    WeeklyActiveGroups,
    MonthlyActiveGroups,
    CountPerGroup {
        aggregate: AggregateFunction,
    },
    #[serde(rename_all = "camelCase")]
    AggregatePropertyPerGroup {
        property_name: String,
        property_type: PropertyType,
        aggregate_per_group: PartitionedAggregateFunction,
        aggregate: AggregateFunction,
    },
    #[serde(rename_all = "camelCase")]
    AggregateProperty {
        property_name: String,
        property_type: PropertyType,
        aggregate: AggregateFunction,
    },
    QueryFormula {
        formula: String,
    },
}

impl TryInto<query_es_types::Query> for &Query {
    type Error = Error;

    fn try_into(self) -> std::result::Result<query_es_types::Query, Self::Error> {
        Ok(match self {
            Query::CountEvents => query_es_types::Query::CountEvents,
            Query::CountUniqueGroups => query_es_types::Query::CountUniqueGroups,
            Query::DailyActiveGroups => query_es_types::Query::DailyActiveGroups,
            Query::WeeklyActiveGroups => query_es_types::Query::WeeklyActiveGroups,
            Query::MonthlyActiveGroups => query_es_types::Query::MonthlyActiveGroups,
            Query::CountPerGroup { aggregate } => query_es_types::Query::CountPerGroup { aggregate: aggregate.try_into()? },
            Query::AggregatePropertyPerGroup {
                property_name,
                property_type,
                aggregate_per_group,
                aggregate
            } => query_es_types::Query::AggregatePropertyPerGroup {
                property: convert_property(property_type.clone(), property_name.clone()),
                aggregate_per_group: aggregate_per_group.try_into()?,
                aggregate: aggregate.try_into()?,
            },
            Query::AggregateProperty {
                property_name,
                property_type,
                aggregate
            } => query_es_types::Query::AggregateProperty {
                property: convert_property(property_type.clone(), property_name.clone()),
                aggregate: aggregate.try_into()?,
            },
            Query::QueryFormula { formula } => query_es_types::Query::QueryFormula { formula: formula.clone() }
        })
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PropertyType {
    User,
    Event,
    Custom,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum EventFilter {
    #[serde(rename_all = "camelCase")]
    Property {
        property_name: String,
        property_type: PropertyType,
        operation: PropValueOperation,
        #[serde(skip_serializing_if = "Option::is_none")]
        value: Option<Vec<Value>>,
    },
}

impl TryInto<query_es_types::EventFilter> for &EventFilter {
    type Error = Error;

    fn try_into(self) -> std::result::Result<query_es_types::EventFilter, Self::Error> {
        Ok(match self {
            EventFilter::Property {
                property_name,
                property_type,
                operation,
                value
            } => query_es_types::EventFilter::Property {
                property: convert_property(property_type.clone(), property_name.clone()),
                operation: operation.try_into()?,
                value: match value {
                    None => None,
                    Some(v) => Some(v.iter().map(|v| try_convert_value(v)).collect::<Result<_>>()?)
                },
            }
        })
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum Breakdown {
    #[serde(rename_all = "camelCase")]
    Property {
        property_name: String,
        property_type: PropertyType,
    },
}

impl TryInto<query_es_types::Breakdown> for &Breakdown {
    type Error = Error;

    fn try_into(self) -> std::result::Result<query_es_types::Breakdown, Self::Error> {
        Ok(match self {
            Breakdown::Property { property_name, property_type } =>
                query_es_types::Breakdown::Property(convert_property(property_type.clone(), property_name.clone())),
        })
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub enum EventType {
    Regular,
    Custom,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Event {
    pub event_name: String,
    pub event_type: EventType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filters: Option<Vec<EventFilter>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub breakdowns: Option<Vec<Breakdown>>,
    pub queries: Vec<Query>,
}

impl TryInto<query_es_types::Event> for &Event {
    type Error = Error;

    fn try_into(self) -> std::result::Result<query_es_types::Event, Self::Error> {
        Ok(query_es_types::Event {
            event: match self.event_type {
                EventType::Regular => query_types::EventRef::Regular(self.event_name.clone()),
                EventType::Custom => query_types::EventRef::Custom(self.event_name.clone()),
            },
            filters: self.filters.as_ref().map(|v| v.iter().map(|v| v.try_into()).collect::<std::result::Result<_, _>>()).transpose()?,
            breakdowns: self.breakdowns.as_ref().map(|v| v.iter().map(|v| v.try_into()).collect::<std::result::Result<_, _>>()).transpose()?,
            queries: self.queries
                .iter()
                .map(|v| v.try_into())
                .collect::<std::result::Result<Vec<query_es_types::Query>, _>>()?
                .iter()
                .enumerate()
                .map(|(idx, v)| NamedQuery::new(v.clone(), Some(format!("{}_{:?}_{}", self.event_name.to_case(Case::Snake), self.event_type, idx)))).collect(),
        })
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum SegmentCondition {}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Segment {
    name: String,
    conditions: Vec<SegmentCondition>,
}


#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventSegmentation {
    pub time: QueryTime,
    pub group: String,
    pub interval_unit: TimeUnit,
    pub chart_type: ChartType,
    pub analysis: Analysis,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compare: Option<Compare>,
    pub events: Vec<Event>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filters: Option<Vec<EventFilter>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub breakdowns: Option<Vec<Breakdown>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub segments: Option<Vec<Segment>>,
}

impl TryInto<query_es_types::EventSegmentation> for EventSegmentation {
    type Error = Error;

    fn try_into(self) -> std::result::Result<query_es_types::EventSegmentation, Self::Error> {
        Ok(query_es_types::EventSegmentation {
            time: self.time.try_into()?,
            group: self.group,
            interval_unit: self.interval_unit.try_into()?,
            chart_type: self.chart_type.try_into()?,
            analysis: self.analysis.try_into()?,
            compare: self.compare.map(|v| v.try_into()).transpose()?,
            events: self.events.iter().map(|v| v.try_into()).collect::<std::result::Result<_, _>>()?,
            filters: self.filters.map(|v| v.iter().map(|v| v.try_into()).collect::<std::result::Result<_, _>>()).transpose()?,
            breakdowns: self.breakdowns.map(|v| v.iter().map(|v| v.try_into()).collect::<std::result::Result<_, _>>()).transpose()?,
            segments: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use chrono::{DateTime, Utc};
    use serde_json::{json, Value};
    use common::ScalarValue;
    use query::event_fields;
    use crate::event_segmentation::types::{AggregateFunction, Analysis, Breakdown, ChartType, Compare, Event, EventFilter, EventSegmentation, EventType, PartitionedAggregateFunction, PropertyType, PropValueOperation, Query, QueryTime, TimeUnit};
    use query::reports::event_segmentation::types::EventSegmentation as QueryEventSegmentation;
    use crate::error::Result;

    #[test]
    fn test_serialize() -> Result<()> {
        let from = DateTime::parse_from_rfc3339("2021-09-08T13:42:00.000000+00:00")
            .unwrap()
            .with_timezone(&Utc);
        let to = DateTime::parse_from_rfc3339("2021-09-08T13:48:00.000000+00:00")
            .unwrap()
            .with_timezone(&Utc);
        let es = EventSegmentation {
            time: QueryTime::Between {
                from,
                to,
            },
            group: event_fields::USER_ID.to_string(),
            interval_unit: TimeUnit::Minute,
            chart_type: ChartType::Line,
            analysis: Analysis::Linear,
            compare: Some(Compare { offset: 1, unit: TimeUnit::Second }),
            events: vec![
                Event {
                    event_name: "e1".to_string(),
                    event_type: EventType::Regular,
                    filters: Some(vec![
                        EventFilter::Property {
                            property_name: "p1".to_string(),
                            property_type: PropertyType::User,
                            operation: PropValueOperation::Eq,
                            value: Some(vec![json!(true)]),
                        },
                        EventFilter::Property {
                            property_name: "p2".to_string(),
                            property_type: PropertyType::Event,
                            operation: PropValueOperation::Eq,
                            value: Some(vec![json!(true)]),
                        },
                        EventFilter::Property {
                            property_name: "p3".to_string(),
                            property_type: PropertyType::Event,
                            operation: PropValueOperation::Empty,
                            value: None,
                        },
                        EventFilter::Property {
                            property_name: "p4".to_string(),
                            property_type: PropertyType::Event,
                            operation: PropValueOperation::Eq,
                            value: Some(vec![json!("s")]),
                        },
                    ]),
                    breakdowns: Some(vec![
                        Breakdown::Property {
                            property_name: "Device".to_string(),
                            property_type: PropertyType::User,
                        }
                    ]),
                    queries: vec![
                        Query::CountEvents,
                        Query::CountUniqueGroups,
                        Query::CountPerGroup {
                            aggregate: AggregateFunction::Avg,
                        },
                        Query::AggregatePropertyPerGroup {
                            property_name: "Revenue".to_string(),
                            property_type: PropertyType::Event,
                            aggregate_per_group: PartitionedAggregateFunction::Sum,
                            aggregate: AggregateFunction::Avg,
                        },
                        Query::AggregateProperty {
                            property_name: "Revenue".to_string(),
                            property_type: PropertyType::Event,
                            aggregate: AggregateFunction::Sum,
                        },
                    ],
                },
            ],
            filters: Some(vec![
                EventFilter::Property {
                    property_name: "p1".to_string(),
                    property_type: PropertyType::User,
                    operation: PropValueOperation::Eq,
                    value: Some(vec![json!(true)]),
                },
            ]),
            breakdowns: Some(vec![
                Breakdown::Property {
                    property_name: "Device".to_string(),
                    property_type: PropertyType::User,
                }
            ]),
            segments: None,
        };

        let qes: QueryEventSegmentation = es.clone().try_into()?;
        let j = serde_json::to_string_pretty(&es).unwrap();
        Ok(())
    }
}