use std::collections::HashMap;

use chrono::DateTime;
use chrono::Utc;
use serde::Deserialize;

use crate::sources::http::Context;
use crate::sources::http::PropValue;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrackRequest {
    pub user_id: Option<String>,
    pub anonymous_id: Option<String>,
    pub sent_at: DateTime<Utc>,
    pub context: Context,
    #[serde(rename = "type")]
    pub typ: String,
    pub event: Option<String>,
    #[serde(rename = "traits")]
    pub user_properties: HashMap<String, PropValue>,
}
