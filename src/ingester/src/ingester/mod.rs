//! Ingester entities.

mod error;
pub(crate) mod storage;

use std::collections::HashMap;
use std::net::IpAddr;

use chrono::DateTime;
use chrono::Utc;
pub use error::Error;
use hyper::Uri;
use serde::Deserialize;
use serde::Serialize;

pub(crate) const DEFAULT_USER_ID: u64 = 1;

type Timestamp = i64;

pub(crate) type Properties = HashMap<String, Option<PropValue>>;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TrackRequest {
    pub user_id: String,
    pub sent_at: Option<DateTime<Utc>>,
    pub context: Option<Context>,
    pub event: String,
    pub properties: Option<Properties>,
    pub user_properties: Option<HashMap<String, String>>,
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
    pub library: TrackingLibrary,
    pub page: Option<Page>,
    pub user_agent: String,
    pub ip: IpAddr,
}

#[derive(Debug, Deserialize)]
pub(crate) struct TrackingLibrary {
    pub name: String,
    pub version: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct Page {
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
