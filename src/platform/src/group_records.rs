use std::collections::HashMap;
use std::sync::Arc;

use axum::async_trait;
use chrono::{DateTime, Utc};
use common::query::Segment;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use common::rbac::ProjectPermission;
use metadata::MetadataProvider;
use query::context::Format;
use query::{event_records, group_records};
use query::group_records::fix_search_request;
use query::group_records::GroupRecordsProvider;

use crate::{Context, PlatformError, PropertyAndValue, PropertyRef, PropValueFilter, QueryParams, QueryResponse, QueryResponseFormat, QueryTime, SortablePropertyRef, validate_event, validate_event_filter};
use crate::event_records::{EventRecord, EventRecordsSearchRequest};
use crate::EventGroupedFilters;
use crate::ListResponse;
use crate::Result;

pub struct GroupRecords {
    md: Arc<MetadataProvider>,
    prov: Arc<GroupRecordsProvider>,
}

impl GroupRecords {
    pub fn new(md: Arc<MetadataProvider>, prov: Arc<GroupRecordsProvider>) -> Self {
        Self { md, prov }
    }

    pub async fn get_by_id(&self, ctx: Context, project_id: u64, group_id: usize, id: String) -> Result<GroupRecord> {
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

        let mut data = self.prov.get_by_id(ctx, group_id, id).await?;

        Ok(data.into())
    }

    pub async fn search(&self, ctx: Context, project_id: u64, req: GroupRecordsSearchRequest, query: QueryParams) -> Result<QueryResponse> {
        ctx.check_project_permission(
            ctx.organization_id,
            project_id,
            ProjectPermission::ExploreReports,
        )?;
        validate_search_request(&self.md, project_id, &req)?;
        let lreq = fix_search_request(&self.md, project_id, req.into())?;
        let cur_time = match query.timestamp {
            None => Utc::now(),
            Some(ts_sec) => DateTime::from_naive_utc_and_offset(
                chrono::NaiveDateTime::from_timestamp_millis(ts_sec * 1000).unwrap(),
                Utc,
            ),
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

        let mut data = self.prov.search(ctx, lreq.into()).await?;

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


impl Into<group_records::GroupRecordsSearchRequest> for GroupRecordsSearchRequest {
    fn into(self) -> group_records::GroupRecordsSearchRequest {
        group_records::GroupRecordsSearchRequest {
            time: self.time.map(|t| t.into()),
            group_id: self.group,
            filters: self.filters.map_or_else(|| None, |v| {
                if v.groups[0].filters.is_empty() {
                    None
                } else {
                    Some(v.groups[0].filters.iter().map(|v| v.to_owned().into()).collect::<Vec<_>>())
                }
            }),
            properties: self.properties.map(|props| {
                props
                    .iter()
                    .map(|prop| prop.to_owned().into())
                    .collect::<Vec<_>>()
            }),
            sort: None,
        }
    }
}

pub(crate) fn validate_search_request(
    md: &Arc<MetadataProvider>,
    project_id: u64,
    req: &GroupRecordsSearchRequest,
) -> Result<()> {
    match &req.time {
        None => {}
        Some(time) => match time {
            QueryTime::Between { from, to } => {
                if from > to {
                    return Err(PlatformError::BadRequest(
                        "from time must be less than to time".to_string(),
                    ));
                }
            }
            _ => {}
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
            if props.is_empty() {
                return Err(PlatformError::BadRequest(
                    "props field can't be empty".to_string(),
                ));
            }
            for (idx, prop) in props.iter().enumerate() {
                match prop {
                    PropertyRef::Group {
                        property_name,
                        group,
                    } => md.group_properties[*group]
                        .get_by_name(project_id, &property_name)
                        .map_err(|err| {
                            PlatformError::BadRequest(format!("property {idx}: {err}"))
                        })?,
                    PropertyRef::Event { property_name } => md
                        .event_properties
                        .get_by_name(project_id, &property_name)
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

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupRecordsSearchRequest {
    pub time: Option<QueryTime>,
    pub group: usize,
    pub filters: Option<EventGroupedFilters>,
    pub properties: Option<Vec<PropertyRef>>,
    pub sort: Option<SortablePropertyRef>,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct GroupRecord {
    pub properties: Vec<PropertyAndValue>,
}

impl Into<GroupRecord> for query::group_records::GroupRecord {
    fn into(self) -> GroupRecord {
        GroupRecord { properties: self.properties.iter().map(|p| p.to_owned().into()).collect::<Vec<_>>() }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateGroupRecordRequest {
    pub properties: HashMap<String, Value>,
}
