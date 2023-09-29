//! Entities related to tracking

use std::collections::HashMap;
use std::net::IpAddr;

use chrono::DateTime;
use chrono::Utc;
use hyper::Uri;
use serde::Deserialize;

type Timestamp = i64;

#[derive(Debug, Deserialize)]
#[serde(rename = "camelCase")]
pub(crate) struct TrackRequest {
    user_id: String,
    sent_at: Option<DateTime<Utc>>,
    context: Option<Context>,
    event: String,
    properties: HashMap<String, Option<PropValue>>,
}

#[derive(Debug, Deserialize)]
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
