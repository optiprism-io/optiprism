use std::sync::Arc;

use common::GROUPS_COUNT;
use metadata::MetadataProvider;
use query::queries;
use serde::Deserialize;
use serde::Serialize;

use crate::queries::validation::validate_event;
use crate::queries::validation::validate_event_filter;
use crate::queries::QueryTime;
use crate::EventRef;
use crate::PlatformError;
use crate::PropValueFilter;
use crate::PropertyRef;
use crate::Result;
use crate::SortablePropertyRef;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Event {
    #[serde(flatten)]
    pub event: EventRef,
    pub filters: Option<Vec<PropValueFilter>>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupRecordsSearchRequest {
    pub time: Option<QueryTime>,
    pub group: usize,
    pub filters: Option<Vec<PropValueFilter>>,
    pub properties: Option<Vec<PropertyRef>>,
    pub sort: Option<SortablePropertyRef>,
}

impl Into<query::queries::group_records_search::GroupRecordsSearch> for GroupRecordsSearchRequest {
    fn into(self) -> query::queries::group_records_search::GroupRecordsSearch {
        query::queries::group_records_search::GroupRecordsSearch {
            time: self.time.map(|v| v.into()),
            group_id: self.group,
            filters: self.filters.map(|filters| {
                filters
                    .iter()
                    .map(|filter| filter.to_owned().into())
                    .collect::<Vec<_>>()
            }),
            properties: self.properties.map(|props| {
                props
                    .iter()
                    .map(|prop| prop.to_owned().into())
                    .collect::<Vec<_>>()
            }),
            sort: self.sort.map(|v| match v {
                SortablePropertyRef::System {
                    property_name,
                    direction,
                } => (common::query::PropertyRef::System(property_name), direction),
                SortablePropertyRef::SystemGroup {
                    property_name,
                    direction,
                } => (
                    common::query::PropertyRef::SystemGroup(property_name),
                    direction,
                ),
                SortablePropertyRef::Group {
                    property_name,
                    group,
                    direction,
                } => (
                    common::query::PropertyRef::Group(property_name, group),
                    direction,
                ),
                SortablePropertyRef::Event {
                    property_name,
                    direction,
                } => (common::query::PropertyRef::Event(property_name), direction),
                SortablePropertyRef::Custom {
                    property_id,
                    direction,
                } => (common::query::PropertyRef::Custom(property_id), direction),
            }),
        }
    }
}

pub(crate) fn validate_request(
    md: &Arc<MetadataProvider>,
    project_id: u64,
    req: &GroupRecordsSearchRequest,
) -> Result<()> {
    if req.group > GROUPS_COUNT - 1 {
        return Err(PlatformError::BadRequest(
            "group id is out of range".to_string(),
        ));
    }
    if let Some(time) = &req.time {
        match time {
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
            for (filter_id, filter) in filters.iter().enumerate() {
                validate_event_filter(md, project_id, filter, filter_id, "".to_string())?;
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
                    PropertyRef::SystemGroup { property_name } => md
                        .system_group_properties
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

    if let Some(prop) = &req.sort {
        match prop {
            SortablePropertyRef::Group {
                property_name,
                group,
                ..
            } => md.group_properties[*group]
                .get_by_name(project_id, &property_name)
                .map_err(|err| PlatformError::BadRequest(format!("{err}")))?,
            SortablePropertyRef::SystemGroup { property_name, .. } => md
                .system_group_properties
                .get_by_name(project_id, &property_name)
                .map_err(|err| PlatformError::BadRequest(format!("{err}")))?,
            _ => {
                return Err(PlatformError::Unimplemented(
                    "invalid property type".to_string(),
                ));
            }
        };
    }

    Ok(())
}

pub(crate) fn fix_request(
    req: queries::group_records_search::GroupRecordsSearch,
) -> Result<queries::group_records_search::GroupRecordsSearch> {
    let mut out = req.clone();

    if let Some(filters) = &req.filters
        && filters.is_empty()
    {
        out.filters = None;
    }

    if let Some(props) = &req.properties
        && props.is_empty()
    {
        out.properties = None;
    }

    Ok(out)
}
