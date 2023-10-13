use std::collections::HashMap;
use std::net::IpAddr;

use chrono::DateTime;
use chrono::Utc;
use hyper::Uri;
use ingester::input::Context as IngesterContext;
use ingester::input::IngesterInput;
use ingester::input::PropValue as IngesterPropValue;
use ingester::input::TrackingLibrary as IngesterTrackingLibrary;
use serde::Deserialize;
use serde::Serialize;

pub const DEFAULT_USER_ID: u64 = 1;

type Timestamp = i64;

pub type Properties = HashMap<String, Option<PropValue>>;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrackRequest {
    pub user_id: String,
    pub sent_at: Option<DateTime<Utc>>,
    pub context: Option<Context>,
    pub event: String,
    pub properties: Option<Properties>,
    pub user_properties: Option<Properties>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum PropValue {
    String(String),
    Float64(f64),
    Bool(bool),
    Date(Timestamp),
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Context {
    pub library: TrackingLibrary,
    pub page: Option<Page>,
    pub user_agent: String,
    pub ip: IpAddr,
}

#[derive(Debug, Deserialize)]
pub struct TrackingLibrary {
    pub name: String,
    pub version: String,
}

#[derive(Debug, Deserialize)]
pub struct Page {
    #[serde(with = "http_serde::uri")]
    pub path: Uri,
    #[serde(with = "http_serde::uri")]
    pub referrer: Uri,
    pub search: String,
    pub title: String,
    #[serde(with = "http_serde::uri")]
    pub url: Uri,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TrackResponse {
    pub id: u64,
}

impl From<u64> for TrackResponse {
    fn from(id: u64) -> Self {
        TrackResponse { id }
    }
}

pub struct ProjectTrackRequest {
    pub organization_id: u64,
    pub project_id: u64,
    pub request: TrackRequest,
}

impl From<ProjectTrackRequest> for IngesterInput {
    fn from(
        ProjectTrackRequest {
            organization_id,
            project_id,
            request,
        }: ProjectTrackRequest,
    ) -> Self {
        IngesterInput {
            user_id: request.user_id,
            event_name: request.event,
            event_properties: request.properties.map(|props| {
                props
                    .into_iter()
                    .map(|(k, v)| (k, v.map(Into::into)))
                    .collect()
            }),
            user_properties: request.user_properties.map(|props| {
                props
                    .into_iter()
                    .map(|(k, v)| (k, v.map(Into::into)))
                    .collect()
            }),
            context: request.context.map(Into::into),
            sent_at: request.sent_at,
            organization_id,
            project_id,
        }
    }
}

impl From<PropValue> for IngesterPropValue {
    fn from(value: PropValue) -> Self {
        match value {
            PropValue::String(x) => IngesterPropValue::String(x),
            PropValue::Float64(x) => IngesterPropValue::Float64(x),
            PropValue::Bool(x) => IngesterPropValue::Bool(x),
            PropValue::Date(x) => IngesterPropValue::Date(x),
        }
    }
}

impl From<Context> for IngesterContext {
    fn from(ctx: Context) -> Self {
        IngesterContext {
            library: ctx.library.into(),
            ip: ctx.ip,
        }
    }
}

impl From<TrackingLibrary> for IngesterTrackingLibrary {
    fn from(lib: TrackingLibrary) -> Self {
        IngesterTrackingLibrary {
            name: lib.name,
            version: lib.version,
        }
    }
}
