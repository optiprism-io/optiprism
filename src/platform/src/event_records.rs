use std::collections::HashMap;

use axum::async_trait;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

use crate::queries::QueryTime;
use crate::Context;
use crate::EventGroupedFilters;
use crate::EventRef;
use crate::ListResponse;
use crate::PropValueFilter;
use crate::Result;

#[async_trait]
pub trait Provider: Sync + Send {
    async fn list(
        &self,
        ctx: Context,

        project_id: u64,
        request: ListEventRecordsRequest,
    ) -> Result<ListResponse<EventRecord>>;

    async fn get_by_id(&self, ctx: Context, project_id: u64, id: u64) -> Result<EventRecord>;
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Event {
    #[serde(flatten)]
    pub event: EventRef,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filters: Option<Vec<PropValueFilter>>,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ListEventRecordsRequest {
    time: QueryTime,
    #[serde(skip_serializing_if = "Option::is_none")]
    search_in_event_properties: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    search_in_user_properties: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    events: Option<Vec<Event>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    filters: Option<EventGroupedFilters>,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct EventRecord {
    pub id: u64,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_properties: Option<HashMap<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_properties: Option<HashMap<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub matched_custom_events: Option<Vec<u64>>,
}
