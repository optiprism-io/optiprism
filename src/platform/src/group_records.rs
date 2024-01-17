use std::collections::HashMap;

use axum::async_trait;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

use crate::queries::event_segmentation::Segment;
use crate::queries::QueryTime;
use crate::Context;
use crate::EventGroupedFilters;
use crate::ListResponse;
use crate::Result;

#[async_trait]
pub trait Provider: Sync + Send {
    async fn list(
        &self,
        ctx: Context,

        project_id: u64,
        request: ListGroupRecordsRequest,
    ) -> Result<ListResponse<GroupRecord>>;

    async fn get_by_id(&self, ctx: Context, project_id: u64, id: u64) -> Result<GroupRecord>;

    async fn update(
        &self,
        ctx: Context,

        project_id: u64,
        id: u64,
        req: UpdateGroupRecordRequest,
    ) -> Result<GroupRecord>;
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ListGroupRecordsRequest {
    time: QueryTime,
    group: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub search_term: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub segments: Option<Vec<Segment>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    filters: Option<EventGroupedFilters>,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct GroupRecord {
    pub id: u64,
    pub str_id: String,
    pub group: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<HashMap<String, Value>>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateGroupRecordRequest {
    pub properties: HashMap<String, Value>,
}
