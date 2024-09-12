use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use common::event_segmentation::NamedQuery;
use common::rbac::ProjectPermission;
use common::GROUPS_COUNT;
use metadata::MetadataProvider;
use query::context::Format;
use query::event_segmentation::fix_request;
use query::event_segmentation::EventSegmentationProvider;
use serde::Deserialize;
use serde::Serialize;

use crate::error::Result;
use crate::validate_event;
use crate::validate_event_filter;
use crate::validate_event_property;
use crate::AggregateFunction;
use crate::Breakdown;
use crate::Context;
use crate::EventGroupedFilterGroup;
use crate::EventGroupedFilters;
use crate::EventRef;
use crate::PartitionedAggregateFunction;
use crate::PlatformError;
use crate::PropValueFilter;
use crate::PropertyRef;
use crate::QueryParams;
use crate::QueryResponse;
use crate::QueryResponseFormat;
use crate::QueryTime;
use crate::Segment;
use crate::TimeIntervalUnit;

pub struct EventSegmentation {
    md: Arc<MetadataProvider>,
    prov: Arc<EventSegmentationProvider>,
}

impl EventSegmentation {
    pub fn new(md: Arc<MetadataProvider>, prov: Arc<EventSegmentationProvider>) -> Self {
        Self { md, prov }
    }
    pub async fn event_segmentation(
        &self,
        ctx: Context,
        project_id: u64,
        req: EventSegmentationRequest,
        query: QueryParams,
    ) -> Result<QueryResponse> {
        ctx.check_project_permission(
            ctx.organization_id,
            project_id,
            ProjectPermission::ExploreReports,
        )?;
        validate_request(&self.md, project_id, &req)?;
        let lreq = req.into();
        let lreq = fix_request(&self.md, project_id, lreq)?;
        dbg!(&lreq);

        let cur_time = match query.timestamp {
            None => Utc::now(),
            Some(ts_sec) => DateTime::from_timestamp_millis(ts_sec * 1000).unwrap(),
        };
        let ctx = query::Context {
            project_id,
            format: match &query.format {
                None => Format::Regular,
                Some(format) => match format {
                    QueryResponseFormat::Json => Format::Regular,
                    QueryResponseFormat::JsonCompact => Format::Compact,
                },
            },
            cur_time,
        };

        let mut data = self.prov.event_segmentation(ctx, lreq).await?;

        // do empty response so it will be [] instead of [[],[],[],...]
        if !data.columns.is_empty() && data.columns[0].data.is_empty() {
            data.columns = vec![];
        }
        let resp = match query.format {
            None => QueryResponse::columns_to_json(data.columns),
            Some(QueryResponseFormat::Json) => QueryResponse::columns_to_json(data.columns),
            Some(QueryResponseFormat::JsonCompact) => {
                QueryResponse::columns_to_json_compact(data.columns)
            }
        }?;

        Ok(resp)
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ChartType {
    Line,
    Column,
    Pie,
}

#[allow(clippy::all)]
impl Into<common::event_segmentation::ChartType> for ChartType {
    fn into(self) -> common::event_segmentation::ChartType {
        match self {
            ChartType::Line => common::event_segmentation::ChartType::Line,
            ChartType::Column => common::event_segmentation::ChartType::Column,
            ChartType::Pie => common::event_segmentation::ChartType::Pie,
        }
    }
}

#[allow(clippy::all)]
impl Into<ChartType> for common::event_segmentation::ChartType {
    fn into(self) -> ChartType {
        match self {
            common::event_segmentation::ChartType::Line => ChartType::Line,
            common::event_segmentation::ChartType::Column => ChartType::Column,
            common::event_segmentation::ChartType::Pie => ChartType::Pie,
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

#[allow(clippy::all)]
impl Into<common::event_segmentation::Analysis> for Analysis {
    fn into(self) -> common::event_segmentation::Analysis {
        match self {
            Analysis::Linear => common::event_segmentation::Analysis::Linear,
            Analysis::RollingAverage { window, unit } => {
                common::event_segmentation::Analysis::RollingAverage {
                    window,
                    unit: unit.into(),
                }
            }
            Analysis::Logarithmic => common::event_segmentation::Analysis::Logarithmic,
            Analysis::Cumulative => common::event_segmentation::Analysis::Cumulative,
        }
    }
}

#[allow(clippy::all)]
impl Into<Analysis> for common::event_segmentation::Analysis {
    fn into(self) -> Analysis {
        match self {
            common::event_segmentation::Analysis::Linear => Analysis::Linear,
            common::event_segmentation::Analysis::RollingAverage { window, unit } => {
                Analysis::RollingAverage {
                    window,
                    unit: unit.into(),
                }
            }
            common::event_segmentation::Analysis::Logarithmic => Analysis::Logarithmic,
            common::event_segmentation::Analysis::Cumulative => Analysis::Cumulative,
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Compare {
    pub offset: usize,
    pub unit: TimeIntervalUnit,
}

#[allow(clippy::all)]
impl Into<common::event_segmentation::Compare> for Compare {
    fn into(self) -> common::event_segmentation::Compare {
        common::event_segmentation::Compare {
            offset: self.offset,
            unit: self.unit.into(),
        }
    }
}

#[allow(clippy::all)]
impl Into<Compare> for common::event_segmentation::Compare {
    fn into(self) -> Compare {
        Compare {
            offset: self.offset,
            unit: self.unit.into(),
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

#[allow(clippy::all)]
impl Into<common::event_segmentation::QueryAggregatePerGroup> for QueryAggregatePerGroup {
    fn into(self) -> common::event_segmentation::QueryAggregatePerGroup {
        match self {
            QueryAggregatePerGroup::Min => common::event_segmentation::QueryAggregatePerGroup::Min,
            QueryAggregatePerGroup::Max => common::event_segmentation::QueryAggregatePerGroup::Max,
            QueryAggregatePerGroup::Sum => common::event_segmentation::QueryAggregatePerGroup::Sum,
            QueryAggregatePerGroup::Avg => common::event_segmentation::QueryAggregatePerGroup::Avg,
            QueryAggregatePerGroup::Median => {
                common::event_segmentation::QueryAggregatePerGroup::Median
            }
            QueryAggregatePerGroup::DistinctCount => {
                common::event_segmentation::QueryAggregatePerGroup::DistinctCount
            }
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum QueryPerGroup {
    CountEvents,
}

#[allow(clippy::all)]
impl Into<common::event_segmentation::QueryPerGroup> for QueryPerGroup {
    fn into(self) -> common::event_segmentation::QueryPerGroup {
        match self {
            QueryPerGroup::CountEvents => common::event_segmentation::QueryPerGroup::CountEvents,
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

#[allow(clippy::all)]
impl Into<common::event_segmentation::Query> for &Query {
    fn into(self) -> common::event_segmentation::Query {
        match self {
            Query::CountEvents => common::event_segmentation::Query::CountEvents,
            Query::CountUniqueGroups => common::event_segmentation::Query::CountUniqueGroups,
            Query::DailyActiveGroups => common::event_segmentation::Query::DailyActiveGroups,
            Query::WeeklyActiveGroups => common::event_segmentation::Query::WeeklyActiveGroups,
            Query::MonthlyActiveGroups => common::event_segmentation::Query::MonthlyActiveGroups,
            Query::CountPerGroup { aggregate } => {
                common::event_segmentation::Query::CountPerGroup {
                    aggregate: aggregate.into(),
                }
            }
            Query::AggregatePropertyPerGroup {
                property,
                aggregate_per_group,
                aggregate,
            } => common::event_segmentation::Query::AggregatePropertyPerGroup {
                property: property.to_owned().into(),
                aggregate_per_group: aggregate_per_group.into(),
                aggregate: aggregate.into(),
            },
            Query::AggregateProperty {
                property,
                aggregate,
            } => common::event_segmentation::Query::AggregateProperty {
                property: property.to_owned().into(),
                aggregate: aggregate.into(),
            },
            Query::Formula { formula } => common::event_segmentation::Query::QueryFormula {
                formula: formula.clone(),
            },
        }
    }
}

#[allow(clippy::all)]
impl Into<Query> for common::event_segmentation::Query {
    fn into(self) -> Query {
        match self {
            common::event_segmentation::Query::CountEvents => Query::CountEvents,
            common::event_segmentation::Query::CountUniqueGroups => Query::CountUniqueGroups,
            common::event_segmentation::Query::DailyActiveGroups => Query::DailyActiveGroups,
            common::event_segmentation::Query::WeeklyActiveGroups => Query::WeeklyActiveGroups,
            common::event_segmentation::Query::MonthlyActiveGroups => Query::MonthlyActiveGroups,
            common::event_segmentation::Query::CountPerGroup { aggregate } => {
                Query::CountPerGroup {
                    aggregate: aggregate.into(),
                }
            }
            common::event_segmentation::Query::AggregatePropertyPerGroup {
                property,
                aggregate_per_group,
                aggregate,
            } => Query::AggregatePropertyPerGroup {
                property: property.into(),
                aggregate_per_group: aggregate_per_group.into(),
                aggregate: aggregate.into(),
            },
            common::event_segmentation::Query::AggregateProperty {
                property,
                aggregate,
            } => Query::AggregateProperty {
                property: property.into(),
                aggregate: aggregate.into(),
            },
            common::event_segmentation::Query::QueryFormula { formula } => {
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
    pub filters: Option<Vec<PropValueFilter>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub breakdowns: Option<Vec<Breakdown>>,
    pub queries: Vec<Query>,
}

#[allow(clippy::all)]
impl Into<common::event_segmentation::Event> for &Event {
    fn into(self) -> common::event_segmentation::Event {
        common::event_segmentation::Event {
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
                .collect::<Vec<common::event_segmentation::Query>>()
                .iter()
                .enumerate()
                .map(|(idx, v)| NamedQuery::new(v.clone(), Some(self.event.name(idx))))
                .collect(),
        }
    }
}

#[allow(clippy::all)]
impl Into<Event> for &common::event_segmentation::Event {
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
pub struct EventSegmentationRequest {
    pub time: QueryTime,
    pub group: usize,
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

#[allow(clippy::all)]
impl Into<common::event_segmentation::EventSegmentationRequest> for EventSegmentationRequest {
    fn into(self) -> common::event_segmentation::EventSegmentationRequest {
        common::event_segmentation::EventSegmentationRequest {
            time: self.time.into(),
            group_id: self.group,
            interval_unit: self.interval_unit.into(),
            chart_type: self.chart_type.into(),
            analysis: self.analysis.into(),
            compare: self.compare.map(|v| v.into()),
            events: self.events.iter().map(|v| v.into()).collect::<Vec<_>>(),
            filters: self.filters.map_or_else(
                || None,
                |v| {
                    if v.groups[0].filters.is_empty() {
                        None
                    } else {
                        Some(
                            v.groups[0]
                                .filters
                                .iter()
                                .map(|v| v.to_owned().into())
                                .collect::<Vec<_>>(),
                        )
                    }
                },
            ),
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

#[allow(clippy::all)]
impl Into<EventSegmentationRequest> for common::event_segmentation::EventSegmentationRequest {
    fn into(self) -> EventSegmentationRequest {
        EventSegmentationRequest {
            time: self.time.into(),
            group: self.group_id,
            interval_unit: self.interval_unit.into(),
            chart_type: self.chart_type.into(),
            analysis: self.analysis.into(),
            compare: self.compare.map(|v| v.into()),
            events: self.events.iter().map(|v| v.into()).collect::<Vec<_>>(),
            filters: self.filters.map(|v| {
                let f = v.iter().map(|v| v.to_owned().into()).collect::<Vec<_>>();
                EventGroupedFilters {
                    groups_condition: None,
                    groups: vec![EventGroupedFilterGroup {
                        filters_condition: Default::default(),
                        filters: f,
                    }],
                }
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

pub(crate) fn validate_request(
    md: &Arc<MetadataProvider>,
    project_id: u64,
    req: &EventSegmentationRequest,
) -> Result<()> {
    if req.group > GROUPS_COUNT - 1 {
        return Err(PlatformError::BadRequest(
            "group id is out of range".to_string(),
        ));
    }
    if let QueryTime::Between { from, to } = req.time {
        if from > to {
            return Err(PlatformError::BadRequest(
                "from time must be less than to time".to_string(),
            ));
        }
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
                        validate_event_property(
                            md,
                            project_id,
                            property,
                            format!("breakdown {idx}"),
                        )?;
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

#[cfg(test)]
mod tests {
    use chrono::DateTime;
    use chrono::Utc;
    use common::GROUP_USER_ID;
    use serde_json::json;

    use crate::error::Result;
    use crate::event_segmentation::AggregateFunction;
    use crate::event_segmentation::Analysis;
    use crate::event_segmentation::Breakdown;
    use crate::event_segmentation::ChartType;
    use crate::event_segmentation::Compare;
    use crate::event_segmentation::Event;
    use crate::event_segmentation::EventSegmentationRequest;
    use crate::event_segmentation::PartitionedAggregateFunction;
    use crate::event_segmentation::PropValueFilter;
    use crate::event_segmentation::Query;
    use crate::event_segmentation::QueryTime;
    use crate::event_segmentation::TimeIntervalUnit;
    use crate::{EventGroupedFilterGroup, EventGroupedFilters};
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
        let es = EventSegmentationRequest {
            time: QueryTime::Between { from, to },
            group: GROUP_USER_ID,
            interval_unit: TimeIntervalUnit::Hour,
            chart_type: ChartType::Line,
            analysis: Analysis::Linear,
            compare: Some(Compare {
                offset: 1,
                unit: TimeIntervalUnit::Hour,
            }),
            events: vec![Event {
                event: EventRef::Regular {
                    event_name: "e1".to_string(),
                },
                filters: Some(vec![
                    PropValueFilter::Property {
                        property: PropertyRef::Group {
                            property_name: "p1".to_string(),
                            group: 0,
                        },
                        operation: PropValueOperation::Eq,
                        value: Some(vec![json!(true)]),
                    },
                    PropValueFilter::Property {
                        property: PropertyRef::Event {
                            property_name: "p2".to_string(),
                        },
                        operation: PropValueOperation::Eq,
                        value: Some(vec![json!(true)]),
                    },
                    PropValueFilter::Property {
                        property: PropertyRef::Event {
                            property_name: "p3".to_string(),
                        },
                        operation: PropValueOperation::Empty,
                        value: None,
                    },
                    PropValueFilter::Property {
                        property: PropertyRef::Event {
                            property_name: "p4".to_string(),
                        },
                        operation: PropValueOperation::Eq,
                        value: Some(vec![json!("s")]),
                    },
                ]),
                breakdowns: Some(vec![Breakdown::Property {
                    property: PropertyRef::Group {
                        property_name: "Device".to_string(),
                        group: 0,
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
                groups: vec![EventGroupedFilterGroup {
                    filters_condition: Default::default(),
                    filters: vec![PropValueFilter::Property {
                        property: PropertyRef::Group {
                            property_name: "p1".to_string(),
                            group: 0,
                        },
                        operation: PropValueOperation::Eq,
                        value: Some(vec![json!(true)]),
                    }, ],
                }],
            }),
            // filters: Some(vec![EventFilter::Property {
            // property: PropertyRef::User {
            // property_name: "p1".to_string(),
            // },
            // operation: PropValueOperation::Eq,
            // value: Some(vec![json!(true)]),
            // }]),
            breakdowns: Some(vec![Breakdown::Property {
                property: PropertyRef::Group {
                    property_name: "Device".to_string(),
                    group: 0,
                },
            }]),
            segments: None,
        };

        let _qes: common::event_segmentation::EventSegmentationRequest = es.clone().into();
        let j = serde_json::to_string_pretty(&es).unwrap();
        print!("{}", j);

        Ok(())
    }
}
