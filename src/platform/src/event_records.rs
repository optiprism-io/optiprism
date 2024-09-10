use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use common::rbac::ProjectPermission;
use metadata::MetadataProvider;
use query::context::Format;
use query::event_records;
use query::event_records::fix_search_request;
use serde::Deserialize;
use serde::Serialize;

use crate::scalar_to_json;
use crate::validate_event;
use crate::validate_event_filter;
use crate::Context;
use crate::EventGroupedFilters;
use crate::EventRef;
use crate::PlatformError;
use crate::PropValueFilter;
use crate::PropertyAndValue;
use crate::PropertyRef;
use crate::QueryParams;
use crate::QueryResponse;
use crate::QueryResponseFormat;
use crate::QueryTime;
use crate::Result;

pub struct EventRecords {
    md: Arc<MetadataProvider>,
    prov: Arc<event_records::EventRecordsProvider>,
}

impl EventRecords {
    pub fn new(md: Arc<MetadataProvider>, prov: Arc<event_records::EventRecordsProvider>) -> Self {
        Self { md, prov }
    }

    pub async fn get_by_id(&self, ctx: Context, project_id: u64, id: u64) -> Result<EventRecord> {
        ctx.check_project_permission(
            ctx.organization_id,
            project_id,
            ProjectPermission::ExploreReports,
        )?;
        let ctx = query::Context {
            project_id,
            format: Format::Regular,
            cur_time: Utc::now(),
        };

        let data = self.prov.get_by_id(ctx, id).await?;

        Ok(data.into())
    }

    pub async fn search(
        &self,
        ctx: Context,
        project_id: u64,
        req: EventRecordsSearchRequest,
        query: QueryParams,
    ) -> Result<QueryResponse> {
        ctx.check_project_permission(
            ctx.organization_id,
            project_id,
            ProjectPermission::ExploreReports,
        )?;
        validate_search_request(&self.md, project_id, &req)?;
        let lreq = fix_search_request(&self.md, project_id, req.into())?;
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

        let mut data = self.prov.search(ctx, lreq).await?;

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
pub struct EventRecord {
    pub properties: Vec<PropertyAndValue>,
}

#[allow(clippy::all)]
impl Into<EventRecord> for query::event_records::EventRecord {
    fn into(self) -> EventRecord {
        EventRecord {
            properties: self
                .properties
                .iter()
                .map(|p| p.to_owned().into())
                .collect::<Vec<_>>(),
        }
    }
}

#[allow(clippy::all)]
impl Into<PropertyAndValue> for query::PropertyAndValue {
    fn into(self) -> PropertyAndValue {
        let value = scalar_to_json(&self.value);

        PropertyAndValue {
            property: self.property.into(),
            value,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Event {
    #[serde(flatten)]
    pub event: EventRef,
    pub filters: Option<Vec<PropValueFilter>>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventRecordsSearchRequest {
    pub time: QueryTime,
    pub events: Option<Vec<Event>>,
    pub filters: Option<EventGroupedFilters>,
    pub properties: Option<Vec<PropertyRef>>,
}

#[allow(clippy::all)]
impl Into<event_records::Event> for Event {
    fn into(self) -> event_records::Event {
        event_records::Event {
            event: self.event.into(),
            filters: self.filters.map(|filters| {
                filters
                    .iter()
                    .map(|filter| filter.to_owned().into())
                    .collect::<Vec<_>>()
            }),
        }
    }
}

#[allow(clippy::all)]
impl Into<event_records::EventRecordsSearchRequest> for EventRecordsSearchRequest {
    fn into(self) -> event_records::EventRecordsSearchRequest {
        event_records::EventRecordsSearchRequest {
            time: self.time.into(),
            events: self.events.map(|events| {
                events
                    .into_iter()
                    .map(|event| event.into())
                    .collect::<Vec<_>>()
            }),
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
            properties: self.properties.map(|props| {
                props
                    .iter()
                    .map(|prop| prop.to_owned().into())
                    .collect::<Vec<_>>()
            }),
        }
    }
}

pub(crate) fn validate_search_request(
    md: &Arc<MetadataProvider>,
    project_id: u64,
    req: &EventRecordsSearchRequest,
) -> Result<()> {
    if let QueryTime::Between { from, to } = req.time {
        if from > to {
            return Err(PlatformError::BadRequest(
                "from time must be less than to time".to_string(),
            ));
        }
    }

    if let Some(events) = &req.events {
        if events.is_empty() {
            return Err(PlatformError::BadRequest(
                "events field can't be empty".to_string(),
            ));
        }
        for (event_id, event) in events.iter().enumerate() {
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

    match &req.properties {
        None => {}
        Some(props) => {
            for (idx, prop) in props.iter().enumerate() {
                match prop {
                    PropertyRef::Group {
                        property_name,
                        group,
                    } => md.group_properties[*group]
                        .get_by_name(project_id, property_name)
                        .map_err(|err| {
                            PlatformError::BadRequest(format!("property {idx}: {err}"))
                        })?,
                    PropertyRef::Event { property_name } => md
                        .event_properties
                        .get_by_name(project_id, property_name)
                        .map_err(|err| {
                            PlatformError::BadRequest(format!("property {idx}: {err}"))
                        })?,
                    _ => {
                        return Err(PlatformError::Unimplemented(
                            "invalid property type".to_string(),
                        ));
                    }
                };
            }
        }
    }
    Ok(())
}
