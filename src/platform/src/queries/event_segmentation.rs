use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use common::query::event_segmentation::NamedQuery;
use common::types::DType;
use datafusion_common::ScalarValue;
use metadata::MetadataProvider;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

use crate::error::Result;
use crate::json_value_to_scalar;
use crate::queries::validation::validate_event;
use crate::queries::validation::validate_event_filter;
use crate::queries::validation::validate_property;
use crate::queries::AggregateFunction;
use crate::queries::Breakdown;
use crate::queries::PartitionedAggregateFunction;
use crate::queries::QueryTime;
use crate::queries::Segment;
use crate::queries::TimeIntervalUnit;
use crate::scalar_to_json_value;
use crate::Context;
use crate::EventFilter;
use crate::EventGroupedFilterGroup;
use crate::EventGroupedFilters;
use crate::EventRef;
use crate::PlatformError;
use crate::PropValueOperation;
use crate::PropertyRef;

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ChartType {
    Line,
    Column,
    Pie,
}

impl Into<common::query::event_segmentation::ChartType> for ChartType {
    fn into(self) -> common::query::event_segmentation::ChartType {
        match self {
            ChartType::Line => common::query::event_segmentation::ChartType::Line,
            ChartType::Column => common::query::event_segmentation::ChartType::Column,
            ChartType::Pie => common::query::event_segmentation::ChartType::Pie,
        }
    }
}

impl Into<ChartType> for common::query::event_segmentation::ChartType {
    fn into(self) -> ChartType {
        match self {
            common::query::event_segmentation::ChartType::Line => ChartType::Line,
            common::query::event_segmentation::ChartType::Column => ChartType::Column,
            common::query::event_segmentation::ChartType::Pie => ChartType::Pie,
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum Analysis {
    Linear,
    RollingAverage {
        window: usize,
        unit: TimeIntervalUnit,
    },
    Logarithmic,
    Cumulative,
}

impl Into<common::query::event_segmentation::Analysis> for Analysis {
    fn into(self) -> common::query::event_segmentation::Analysis {
        match self {
            Analysis::Linear => common::query::event_segmentation::Analysis::Linear,
            Analysis::RollingAverage { window, unit } => {
                common::query::event_segmentation::Analysis::RollingAverage {
                    window,
                    unit: unit.into(),
                }
            }
            Analysis::Logarithmic => common::query::event_segmentation::Analysis::Logarithmic,
            Analysis::Cumulative => common::query::event_segmentation::Analysis::Cumulative,
        }
    }
}

impl Into<Analysis> for common::query::event_segmentation::Analysis {
    fn into(self) -> Analysis {
        match self {
            common::query::event_segmentation::Analysis::Linear => Analysis::Linear,
            common::query::event_segmentation::Analysis::RollingAverage { window, unit } => {
                Analysis::RollingAverage {
                    window,
                    unit: unit.into(),
                }
            }
            common::query::event_segmentation::Analysis::Logarithmic => Analysis::Logarithmic,
            common::query::event_segmentation::Analysis::Cumulative => Analysis::Cumulative,
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Compare {
    pub offset: usize,
    pub unit: TimeIntervalUnit,
}

impl Into<common::query::event_segmentation::Compare> for Compare {
    fn into(self) -> common::query::event_segmentation::Compare {
        common::query::event_segmentation::Compare {
            offset: self.offset,
            unit: self.unit.into(),
        }
    }
}

impl Into<Compare> for common::query::event_segmentation::Compare {
    fn into(self) -> Compare {
        Compare {
            offset: self.offset,
            unit: self.unit.into(),
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum QueryAggregate {
    Min,
    Max,
    Sum,
    Avg,
    Median,
    DistinctCount,
    Percentile25,
    Percentile75,
    Percentile90,
    Percentile99,
}

impl Into<common::query::event_segmentation::QueryAggregate> for QueryAggregate {
    fn into(self) -> common::query::event_segmentation::QueryAggregate {
        match self {
            QueryAggregate::Min => common::query::event_segmentation::QueryAggregate::Min,
            QueryAggregate::Max => common::query::event_segmentation::QueryAggregate::Max,
            QueryAggregate::Sum => common::query::event_segmentation::QueryAggregate::Sum,
            QueryAggregate::Avg => common::query::event_segmentation::QueryAggregate::Avg,
            QueryAggregate::Median => common::query::event_segmentation::QueryAggregate::Median,
            QueryAggregate::DistinctCount => {
                common::query::event_segmentation::QueryAggregate::DistinctCount
            }
            QueryAggregate::Percentile25 => {
                common::query::event_segmentation::QueryAggregate::Percentile25th
            }
            QueryAggregate::Percentile75 => {
                common::query::event_segmentation::QueryAggregate::Percentile75th
            }
            QueryAggregate::Percentile90 => {
                common::query::event_segmentation::QueryAggregate::Percentile90th
            }
            QueryAggregate::Percentile99 => {
                common::query::event_segmentation::QueryAggregate::Percentile99th
            }
        }
    }
}

impl Into<QueryAggregate> for common::query::event_segmentation::QueryAggregate {
    fn into(self) -> QueryAggregate {
        match self {
            common::query::event_segmentation::QueryAggregate::Min => QueryAggregate::Min,
            common::query::event_segmentation::QueryAggregate::Max => QueryAggregate::Max,
            common::query::event_segmentation::QueryAggregate::Sum => QueryAggregate::Sum,
            common::query::event_segmentation::QueryAggregate::Avg => QueryAggregate::Avg,
            common::query::event_segmentation::QueryAggregate::Median => QueryAggregate::Median,
            common::query::event_segmentation::QueryAggregate::DistinctCount => {
                QueryAggregate::DistinctCount
            }
            common::query::event_segmentation::QueryAggregate::Percentile25th => {
                QueryAggregate::Percentile25
            }
            common::query::event_segmentation::QueryAggregate::Percentile75th => {
                QueryAggregate::Percentile75
            }
            common::query::event_segmentation::QueryAggregate::Percentile90th => {
                QueryAggregate::Percentile90
            }
            common::query::event_segmentation::QueryAggregate::Percentile99th => {
                QueryAggregate::Percentile99
            }
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum QueryAggregatePerGroup {
    Min,
    Max,
    Sum,
    Avg,
    Median,
    DistinctCount,
}

impl Into<common::query::event_segmentation::QueryAggregatePerGroup> for QueryAggregatePerGroup {
    fn into(self) -> common::query::event_segmentation::QueryAggregatePerGroup {
        match self {
            QueryAggregatePerGroup::Min => {
                common::query::event_segmentation::QueryAggregatePerGroup::Min
            }
            QueryAggregatePerGroup::Max => {
                common::query::event_segmentation::QueryAggregatePerGroup::Max
            }
            QueryAggregatePerGroup::Sum => {
                common::query::event_segmentation::QueryAggregatePerGroup::Sum
            }
            QueryAggregatePerGroup::Avg => {
                common::query::event_segmentation::QueryAggregatePerGroup::Avg
            }
            QueryAggregatePerGroup::Median => {
                common::query::event_segmentation::QueryAggregatePerGroup::Median
            }
            QueryAggregatePerGroup::DistinctCount => {
                common::query::event_segmentation::QueryAggregatePerGroup::DistinctCount
            }
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum QueryPerGroup {
    CountEvents,
}

impl Into<common::query::event_segmentation::QueryPerGroup> for QueryPerGroup {
    fn into(self) -> common::query::event_segmentation::QueryPerGroup {
        match self {
            QueryPerGroup::CountEvents => {
                common::query::event_segmentation::QueryPerGroup::CountEvents
            }
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
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
        #[serde(flatten)]
        property: PropertyRef,
        aggregate_per_group: PartitionedAggregateFunction,
        aggregate: AggregateFunction,
    },
    #[serde(rename_all = "camelCase")]
    AggregateProperty {
        #[serde(flatten)]
        property: PropertyRef,
        aggregate: AggregateFunction,
    },
    Formula {
        formula: String,
    },
}

impl Into<common::query::event_segmentation::Query> for &Query {
    fn into(self) -> common::query::event_segmentation::Query {
        match self {
            Query::CountEvents => common::query::event_segmentation::Query::CountEvents,
            Query::CountUniqueGroups => common::query::event_segmentation::Query::CountUniqueGroups,
            Query::DailyActiveGroups => common::query::event_segmentation::Query::DailyActiveGroups,
            Query::WeeklyActiveGroups => {
                common::query::event_segmentation::Query::WeeklyActiveGroups
            }
            Query::MonthlyActiveGroups => {
                common::query::event_segmentation::Query::MonthlyActiveGroups
            }
            Query::CountPerGroup { aggregate } => {
                common::query::event_segmentation::Query::CountPerGroup {
                    aggregate: aggregate.into(),
                }
            }
            Query::AggregatePropertyPerGroup {
                property,
                aggregate_per_group,
                aggregate,
            } => common::query::event_segmentation::Query::AggregatePropertyPerGroup {
                property: property.to_owned().into(),
                aggregate_per_group: aggregate_per_group.into(),
                aggregate: aggregate.into(),
            },
            Query::AggregateProperty {
                property,
                aggregate,
            } => common::query::event_segmentation::Query::AggregateProperty {
                property: property.to_owned().into(),
                aggregate: aggregate.into(),
            },
            Query::Formula { formula } => common::query::event_segmentation::Query::QueryFormula {
                formula: formula.clone(),
            },
        }
    }
}

impl Into<Query> for common::query::event_segmentation::Query {
    fn into(self) -> Query {
        match self {
            common::query::event_segmentation::Query::CountEvents => Query::CountEvents,
            common::query::event_segmentation::Query::CountUniqueGroups => Query::CountUniqueGroups,
            common::query::event_segmentation::Query::DailyActiveGroups => Query::DailyActiveGroups,
            common::query::event_segmentation::Query::WeeklyActiveGroups => {
                Query::WeeklyActiveGroups
            }
            common::query::event_segmentation::Query::MonthlyActiveGroups => {
                Query::MonthlyActiveGroups
            }
            common::query::event_segmentation::Query::CountPerGroup { aggregate } => {
                Query::CountPerGroup {
                    aggregate: aggregate.into(),
                }
            }
            common::query::event_segmentation::Query::AggregatePropertyPerGroup {
                property,
                aggregate_per_group,
                aggregate,
            } => Query::AggregatePropertyPerGroup {
                property: property.into(),
                aggregate_per_group: aggregate_per_group.into(),
                aggregate: aggregate.into(),
            },
            common::query::event_segmentation::Query::AggregateProperty {
                property,
                aggregate,
            } => Query::AggregateProperty {
                property: property.into(),
                aggregate: aggregate.into(),
            },
            common::query::event_segmentation::Query::QueryFormula { formula } => {
                Query::Formula { formula }
            }
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub enum EventType {
    Regular,
    Custom,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Event {
    #[serde(flatten)]
    pub event: EventRef,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filters: Option<Vec<EventFilter>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub breakdowns: Option<Vec<Breakdown>>,
    pub queries: Vec<Query>,
}

impl Into<common::query::event_segmentation::Event> for &Event {
    fn into(self) -> common::query::event_segmentation::Event {
        common::query::event_segmentation::Event {
            event: self.event.to_owned().into(),
            filters: self.filters.as_ref().map_or_else(
                || None,
                |v| {
                    if v.is_empty() {
                        None
                    } else {
                        Some(v.iter().map(|v| v.to_owned().into()).collect::<Vec<_>>())
                    }
                },
            ),
            breakdowns: self.breakdowns.as_ref().map_or_else(
                || None,
                |v| {
                    if v.is_empty() {
                        None
                    } else {
                        Some(v.iter().map(|v| v.to_owned().into()).collect::<Vec<_>>())
                    }
                },
            ),
            queries: self
                .queries
                .iter()
                .map(|v| v.into())
                .collect::<Vec<common::query::event_segmentation::Query>>()
                .iter()
                .enumerate()
                .map(|(idx, v)| NamedQuery::new(v.clone(), Some(self.event.name(idx))))
                .collect(),
        }
    }
}

impl Into<Event> for &common::query::event_segmentation::Event {
    fn into(self) -> Event {
        Event {
            event: self.event.to_owned().into(),
            filters: self.filters.as_ref().map_or_else(
                || None,
                |v| {
                    if v.is_empty() {
                        None
                    } else {
                        Some(v.iter().map(|v| v.to_owned().into()).collect::<Vec<_>>())
                    }
                },
            ),
            breakdowns: self.breakdowns.as_ref().map_or_else(
                || None,
                |v| {
                    if v.is_empty() {
                        None
                    } else {
                        Some(v.iter().map(|v| v.to_owned().into()).collect::<Vec<_>>())
                    }
                },
            ),
            queries: self
                .queries
                .iter()
                .map(|v| v.agg.clone().into())
                .collect::<Vec<Query>>(),
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct EventSegmentation {
    pub time: QueryTime,
    pub group: String,
    pub interval_unit: TimeIntervalUnit,
    pub chart_type: ChartType,
    pub analysis: Analysis,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compare: Option<Compare>,
    pub events: Vec<Event>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filters: Option<EventGroupedFilters>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub breakdowns: Option<Vec<Breakdown>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub segments: Option<Vec<Segment>>,
}

// impl TryInto<common::query::event_segmentation::SegmentCondition> for SegmentCondition {
// type Error = PlatformError;
//
// fn try_into(self) -> Result<common::query::event_segmentation::SegmentCondition, Self::Error> {
// match self {
// SegmentCondition::HasPropertyValue {
// property_name,
// operation,
// value,
// } => common::query::event_segmentation::SegmentCondition::HasPropertyValue {
// property_name,
// operation: operation.try_into()?,
// value: value
// .map(|v| v.iter().map(json_value_to_scalar))
// .collect::<std::result::Result<_, _>>()
// .transpose()?,
// },
// SegmentCondition::HadPropertyValue {
// property_name,
// operation,
// value,
// time,
// } => common::query::event_segmentation::SegmentCondition::HadPropertyValue {
// property_name,
// operation: operation.try_into()?,
// value: value
// .map(|v| v.iter().map(json_value_to_scalar))
// .collect::<Result<Vec<_>, _>>()
// .transpose()?,
// time: time.try_into()?,
// },
// SegmentCondition::DidEvent {
// event,
// filters,
// aggregate,
// } => common::query::event_segmentation::SegmentCondition::DidEvent {
// event: event.try_into()?,
// filters: filters
// .map(|v| v.iter().map(|v| v.try_into()))
// .collect::<std::result::Result<Vec<_>, _>>()
// .transpose()?,
// aggregate: aggregate.try_into()?,
// },
// }
// }
// }
//
// impl TryInto<SegmentCondition> for common::query::event_segmentation::SegmentCondition {
// type Error = PlatformError;
//
// fn try_into(self) -> Result<SegmentCondition, Self::Error> {
// match self {
// common::query::event_segmentation::SegmentCondition::HasPropertyValue {
// property_name,
// operation,
// value,
// } => SegmentCondition::HasPropertyValue {
// property_name,
// operation: operation.try_into()?,
// value: value
// .map(|v| v.iter().map(|v| json_value_to_scalar(v)))
// .collect::<std::result::Result<_, _>>()
// .transpose()?,
// },
// common::query::event_segmentation::SegmentCondition::HadPropertyValue {
// property_name,
// operation,
// value,
// time,
// } => SegmentCondition::HadPropertyValue {
// property_name,
// operation: operation.try_into()?,
// value: value
// .map(|v| v.iter().map(|v| json_value_to_scalar(v)))
// .collect::<std::result::Result<_, _>>()
// .transpose()?,
// time: time.try_into()?,
// },
// SegmentCondition::DidEvent {
// event,
// filters,
// aggregate,
// } => common::query::event_segmentation::SegmentCondition::DidEvent {
// event: event.try_into()?,
// filters: filters
// .map(|v| v.iter().map(|v| scalar_to_json_value(v)))
// .collect::<std::result::Result<_, _>>()
// .transpose()?,
// aggregate: aggregate.try_into()?,
// },
// }
// }
// }

impl Into<common::query::event_segmentation::EventSegmentation> for EventSegmentation {
    fn into(self) -> common::query::event_segmentation::EventSegmentation {
        common::query::event_segmentation::EventSegmentation {
            time: self.time.into(),
            group: self.group,
            interval_unit: self.interval_unit.into(),
            chart_type: self.chart_type.into(),
            analysis: self.analysis.into(),
            compare: self.compare.map(|v| v.into()),
            events: self.events.iter().map(|v| v.into()).collect::<Vec<_>>(),
            filters: self.filters.map(|v| {
                v.groups[0]
                    .filters
                    .iter()
                    .map(|f| f.to_owned().into())
                    .collect::<Vec<_>>()
            }),
            breakdowns: self.breakdowns.map_or_else(
                || None,
                |v| {
                    if v.is_empty() {
                        None
                    } else {
                        Some(v.iter().map(|v| v.to_owned().into()).collect::<Vec<_>>())
                    }
                },
            ),
            segments: self.segments.map_or_else(
                || None,
                |v| {
                    if v.is_empty() {
                        None
                    } else {
                        Some(v.iter().map(|v| v.to_owned().into()).collect::<Vec<_>>())
                    }
                },
            ),
        }
    }
}

impl Into<EventSegmentation> for common::query::event_segmentation::EventSegmentation {
    fn into(self) -> EventSegmentation {
        EventSegmentation {
            time: self.time.into(),
            group: self.group,
            interval_unit: self.interval_unit.into(),
            chart_type: self.chart_type.into(),
            analysis: self.analysis.into(),
            compare: self.compare.map(|v| v.into()),
            events: self.events.iter().map(|v| v.into()).collect::<Vec<_>>(),
            filters: self.filters.map(|v| {
                let f = v.iter().map(|v| v.to_owned().into()).collect::<Vec<_>>();
                let r = EventGroupedFilters {
                    groups_condition: None,
                    groups: vec![EventGroupedFilterGroup {
                        filters_condition: Default::default(),
                        filters: f,
                    }],
                };
                r
            }),
            breakdowns: self.breakdowns.map_or_else(
                || None,
                |v| {
                    if v.is_empty() {
                        None
                    } else {
                        Some(v.iter().map(|v| v.to_owned().into()).collect::<Vec<_>>())
                    }
                },
            ),
            segments: self
                .segments
                .map(|v| v.iter().map(|v| v.to_owned().into()).collect::<Vec<_>>()),
        }
    }
}

pub(crate) fn validate(
    md: &Arc<MetadataProvider>,
    project_id: u64,
    req: &EventSegmentation,
) -> Result<()> {
    match req.time {
        QueryTime::Between { from, to } => {
            if from > to {
                return Err(PlatformError::BadRequest(
                    "from time must be less than to time".to_string(),
                ));
            }
        }
        _ => {}
    }

    if req.group == "" {
        return Err(PlatformError::BadRequest(
            "group cannot be empty".to_string(),
        ));
    }

    if req.events.is_empty() {
        return Err(PlatformError::BadRequest(
            "events cannot be empty".to_string(),
        ));
    }
    for (event_id, event) in req.events.iter().enumerate() {
        validate_event(md, project_id, &event.event, event_id, "".to_string())?;

        match &event.filters {
            Some(filters) => {
                for (filter_id, filter) in filters.iter().enumerate() {
                    validate_event_filter(
                        md,
                        project_id,
                        filter,
                        filter_id,
                        format!("event #{event_id}, "),
                    )?;
                }
            }
            None => {}
        }

        if event.queries.is_empty() {
            return Err(PlatformError::BadRequest(
                format!("event #{event_id}, \"queries\" field can't be empty").to_string(),
            ));
        }
    }

    match &req.filters {
        None => {}
        Some(filters) => {
            if filters.groups.is_empty() {
                return Err(PlatformError::BadRequest(
                    "filters field can't be empty".to_string(),
                ));
            }
            for filter_group in &filters.groups {
                if filters.groups.is_empty() {
                    return Err(PlatformError::BadRequest(
                        "filter_group field can't be empty".to_string(),
                    ));
                }
                if filter_group.filters.is_empty() {
                    return Err(PlatformError::BadRequest(
                        "filters field can't be empty".to_string(),
                    ));
                }
                for (filter_id, filter) in filter_group.filters.iter().enumerate() {
                    validate_event_filter(md, project_id, filter, filter_id, "".to_string())?;
                }
            }
        }
    }

    match &req.breakdowns {
        None => {}
        Some(breakdowns) => {
            for (idx, breakdown) in breakdowns.iter().enumerate() {
                match breakdown {
                    Breakdown::Property { property } => {
                        validate_property(md, project_id, property, format!("breakdown {idx}"))?;
                    }
                }
            }
        }
    }

    if req.segments.is_some() {
        return Err(PlatformError::Unimplemented(
            "segments are unimplemented yet".to_string(),
        ));
    }

    Ok(())
}

pub(crate) fn fix_types(
    md: &Arc<MetadataProvider>,
    project_id: u64,
    req: common::query::event_segmentation::EventSegmentation,
) -> Result<common::query::event_segmentation::EventSegmentation> {
    let mut out = req.clone();
    for (event_id, event) in req.events.iter().enumerate() {
        let filters = if let Some(filters) = &event.filters {
            let mut filters_out = vec![];
            for (filter_id, filter) in filters.iter().enumerate() {
                match filter {
                    common::query::EventFilter::Property {
                        property,
                        value,
                        operation,
                    } => {
                        if value.is_none() {
                            continue;
                        }
                        let prop = match property {
                            common::query::PropertyRef::System(name) => {
                                md.system_properties.get_by_name(project_id, name)?
                            }
                            common::query::PropertyRef::User(name) => {
                                md.system_properties.get_by_name(project_id, name)?
                            }
                            common::query::PropertyRef::Event(name) => {
                                md.system_properties.get_by_name(project_id, name)?
                            }
                            common::query::PropertyRef::Custom(_) => unimplemented!(),
                        };

                        let mut ev = vec![];
                        for value in value.to_owned().unwrap().iter() {
                            match (&prop.data_type, value) {
                                (&DType::Timestamp, &ScalarValue::Decimal128(_, _, _)) => {
                                    match out.events[event_id].clone().filters.unwrap()[filter_id]
                                        .clone()
                                    {
                                        common::query::EventFilter::Property { value, .. } => {
                                            for value in value.unwrap().iter() {
                                                if let ScalarValue::Decimal128(Some(ts), _, _) =
                                                    value
                                                {
                                                    let sv = ScalarValue::TimestampMillisecond(
                                                        Some(*ts as i64),
                                                        None,
                                                    );
                                                    ev.push(sv);
                                                } else {
                                                    unreachable!()
                                                }
                                            }
                                        }
                                    };
                                }
                                _ => ev.push(value.to_owned()),
                            }
                        }

                        let filter = common::query::EventFilter::Property {
                            property: property.to_owned(),
                            operation: operation.to_owned(),
                            value: Some(ev),
                        };
                        filters_out.push(filter);
                    }
                };
            }
            Some(filters_out)
        } else {
            None
        };
        out.events[event_id].filters = filters;
    }

    // TODO make for out.filters

    Ok(out)
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;
    use chrono::Utc;
    use common::types::COLUMN_USER_ID;
    use serde_json::json;

    use crate::error::Result;
    use crate::queries::event_segmentation::AggregateFunction;
    use crate::queries::event_segmentation::Analysis;
    use crate::queries::event_segmentation::Breakdown;
    use crate::queries::event_segmentation::ChartType;
    use crate::queries::event_segmentation::Compare;
    use crate::queries::event_segmentation::Event;
    use crate::queries::event_segmentation::EventFilter;
    use crate::queries::event_segmentation::EventSegmentation;
    use crate::queries::event_segmentation::PartitionedAggregateFunction;
    use crate::queries::event_segmentation::Query;
    use crate::queries::event_segmentation::QueryTime;
    use crate::queries::event_segmentation::TimeIntervalUnit;
    use crate::EventGroupedFilters;
    use crate::EventRef;
    use crate::PropValueOperation;
    use crate::PropertyRef;

    #[test]
    fn test_serialize() -> Result<()> {
        let from = DateTime::parse_from_rfc3339("2021-09-08T13:42:00.000000+00:00")
            .unwrap()
            .with_timezone(&Utc);
        let to = DateTime::parse_from_rfc3339("2021-09-08T13:48:00.000000+00:00")
            .unwrap()
            .with_timezone(&Utc);
        let es = EventSegmentation {
            time: QueryTime::Between { from, to },
            group: COLUMN_USER_ID.to_string(),
            interval_unit: TimeIntervalUnit::Minute,
            chart_type: ChartType::Line,
            analysis: Analysis::Linear,
            compare: Some(Compare {
                offset: 1,
                unit: TimeIntervalUnit::Second,
            }),
            events: vec![Event {
                event: EventRef::Regular {
                    event_name: "e1".to_string(),
                },
                filters: Some(vec![
                    EventFilter::Property {
                        property: PropertyRef::User {
                            property_name: "p1".to_string(),
                        },
                        operation: PropValueOperation::Eq,
                        value: Some(vec![json!(true)]),
                    },
                    EventFilter::Property {
                        property: PropertyRef::Event {
                            property_name: "p2".to_string(),
                        },
                        operation: PropValueOperation::Eq,
                        value: Some(vec![json!(true)]),
                    },
                    EventFilter::Property {
                        property: PropertyRef::Event {
                            property_name: "p3".to_string(),
                        },
                        operation: PropValueOperation::Empty,
                        value: None,
                    },
                    EventFilter::Property {
                        property: PropertyRef::Event {
                            property_name: "p4".to_string(),
                        },
                        operation: PropValueOperation::Eq,
                        value: Some(vec![json!("s")]),
                    },
                ]),
                breakdowns: Some(vec![Breakdown::Property {
                    property: PropertyRef::User {
                        property_name: "Device".to_string(),
                    },
                }]),
                queries: vec![
                    Query::CountEvents,
                    Query::CountUniqueGroups,
                    Query::CountPerGroup {
                        aggregate: AggregateFunction::Avg,
                    },
                    Query::AggregatePropertyPerGroup {
                        property: PropertyRef::Event {
                            property_name: "Revenue".to_string(),
                        },
                        aggregate_per_group: PartitionedAggregateFunction::Sum,
                        aggregate: AggregateFunction::Avg,
                    },
                    Query::AggregateProperty {
                        property: PropertyRef::Event {
                            property_name: "Revenue".to_string(),
                        },
                        aggregate: AggregateFunction::Sum,
                    },
                ],
            }],
            filters: Some(EventGroupedFilters {
                groups_condition: None,
                groups: vec![],
            }),
            // filters: Some(vec![EventFilter::Property {
            // property: PropertyRef::User {
            // property_name: "p1".to_string(),
            // },
            // operation: PropValueOperation::Eq,
            // value: Some(vec![json!(true)]),
            // }]),
            breakdowns: Some(vec![Breakdown::Property {
                property: PropertyRef::User {
                    property_name: "Device".to_string(),
                },
            }]),
            segments: None,
        };

        let _qes: common::query::event_segmentation::EventSegmentation = es.clone().into();
        let j = serde_json::to_string_pretty(&es).unwrap();
        print!("1 {}", j);

        Ok(())
    }
}
