use std::sync::Arc;

use metadata::MetadataProvider;
use serde::Deserialize;
use serde::Serialize;

use crate::queries::validation::validate_event_filter;
use crate::queries::QueryTime;
use crate::EventFilter;
use crate::EventRef;
use crate::PlatformError;
use crate::PropertyRef;
use crate::Result;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Event {
    #[serde(flatten)]
    pub event: EventRef,
    pub filters: Option<Vec<EventFilter>>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventRecordsSearchRequest {
    pub time: QueryTime,
    pub events: Option<Vec<Event>>,
    pub filters: Option<Vec<EventFilter>>,
    pub properties: Option<Vec<PropertyRef>>,
}

impl TryInto<query::queries::event_records_search::Event> for Event {
    type Error = PlatformError;

    fn try_into(
        self,
    ) -> std::result::Result<query::queries::event_records_search::Event, Self::Error> {
        Ok(query::queries::event_records_search::Event {
            event: self.event.into(),
            filters: self
                .filters
                .map(|filters| {
                    filters
                        .iter()
                        .map(|filter| filter.try_into())
                        .collect::<Result<_>>()
                })
                .transpose()?,
        })
    }
}

impl TryInto<query::queries::event_records_search::EventRecordsSearch>
    for EventRecordsSearchRequest
{
    type Error = PlatformError;

    fn try_into(
        self,
    ) -> std::result::Result<query::queries::event_records_search::EventRecordsSearch, Self::Error>
    {
        Ok(query::queries::event_records_search::EventRecordsSearch {
            time: self.time.try_into()?,
            events: self
                .events
                .map(|events| {
                    events
                        .into_iter()
                        .map(|event| event.try_into())
                        .collect::<Result<_>>()
                })
                .transpose()?,
            filters: self
                .filters
                .map(|filters| {
                    filters
                        .iter()
                        .map(|filter| filter.try_into())
                        .collect::<Result<_>>()
                })
                .transpose()?,
            properties: self
                .properties
                .map(|props| {
                    props
                        .iter()
                        .map(|prop| prop.to_owned().try_into())
                        .collect::<Result<_>>()
                })
                .transpose()?,
        })
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
        for (event_id, event) in events.iter().enumerate() {
            match &event.event {
                EventRef::Regular { event_name } => {
                    md.events
                        .get_by_name(project_id, &event_name)
                        .map_err(|err| {
                            PlatformError::BadRequest(format!("event {event_id}: {err}"))
                        })?;
                }
                EventRef::Custom { event_id } => {
                    md.custom_events
                        .get_by_id(project_id, *event_id)
                        .map_err(|err| {
                            PlatformError::BadRequest(format!("event {event_id}: {err}"))
                        })?;
                }
            }

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
                for (idx, prop) in props.iter().enumerate() {
                    match prop {
                        PropertyRef::User { property_name } => md
                            .user_properties
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
                        PropertyRef::Custom { .. } => {
                            return Err(PlatformError::Unimplemented(
                                "custom property is unimplemented".to_string(),
                            ));
                        }
                    };
                }
            }
        }
    }
    Ok(())
}
