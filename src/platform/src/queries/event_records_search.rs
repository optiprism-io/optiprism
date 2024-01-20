use serde::Deserialize;
use serde::Serialize;

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
