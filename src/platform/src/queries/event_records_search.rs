use std::sync::Arc;

use metadata::MetadataProvider;
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
    pub filters: Option<Vec<PropValueFilter>>,
    pub properties: Option<Vec<PropertyRef>>,
}

impl Into<query::queries::event_records_search::Event> for Event {
    fn into(self) -> query::queries::event_records_search::Event {
        query::queries::event_records_search::Event {
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

impl Into<query::queries::event_records_search::EventRecordsSearch> for EventRecordsSearchRequest {
    fn into(self) -> query::queries::event_records_search::EventRecordsSearch {
        query::queries::event_records_search::EventRecordsSearch {
            time: self.time.into(),
            events: self.events.map(|events| {
                events
                    .into_iter()
                    .map(|event| event.into())
                    .collect::<Vec<_>>()
            }),
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
        }
    }
}

pub(crate) fn validate(
    md: &Arc<MetadataProvider>,
    project_id: u64,
    req: &EventRecordsSearchRequest,
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
                    PropertyRef::Event { property_name } => md
                        .event_properties
                        .get_by_name(project_id, &property_name)
                        .map_err(|err| {
                            PlatformError::BadRequest(format!("property {idx}: {err}"))
                        })?,
                    PropertyRef::System { property_name } => md
                        .system_properties
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
