//! Ingester entities.

use std::collections::HashMap;
use std::net::IpAddr;

use chrono::DateTime;
use chrono::Utc;
use hyper::Uri;
use serde::Deserialize;
use serde::Serialize;

type Timestamp = i64;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TrackRequest {
    user_id: String,
    sent_at: Option<DateTime<Utc>>,
    context: Option<Context>,
    event: String,
    properties: Option<HashMap<String, Option<PropValue>>>,
    user_properties: Option<HashMap<String, String>>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub(crate) enum PropValue {
    String(String),
    Float64(f64),
    Bool(bool),
    Date(Timestamp),
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Context {
    library: TrackingLibrary,
    page: Option<Page>,
    user_agent: String,
    ip: IpAddr,
}

#[derive(Debug, Deserialize)]
pub(crate) struct TrackingLibrary {
    name: String,
    version: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct Page {
    #[serde(with = "http_serde::uri")]
    path: Uri,
    #[serde(with = "http_serde::uri")]
    referrer: Uri,
    search: String,
    title: String,
    #[serde(with = "http_serde::uri")]
    url: Uri,
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
