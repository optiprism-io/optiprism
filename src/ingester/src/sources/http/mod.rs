use std::collections::HashMap;
use std::net::IpAddr;

use chrono::DateTime;
use chrono::Utc;
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::Destination;

pub mod service;

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Context {
    pub library: Option<Library>,
    pub page: Option<Page>,
    pub user_agent: Option<String>,
    pub ip: Option<IpAddr>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Library {
    pub name: String,
    pub version: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Page {
    pub path: Option<String>,
    pub referrer: Option<String>,
    pub search: Option<String>,
    pub title: Option<String>,
    pub url: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum PropValue {
    Date(DateTime<Utc>),
    String(String),
    Number(Decimal),
    Bool(bool),
}

impl From<PropValue> for crate::PropValue {
    fn from(value: PropValue) -> Self {
        match value {
            PropValue::Date(v) => crate::PropValue::Date(v),
            PropValue::String(v) => crate::PropValue::String(v),
            PropValue::Number(v) => crate::PropValue::Number(v),
            PropValue::Bool(v) => crate::PropValue::Bool(v),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IdentifyRequest {
    pub user_id: Option<String>,
    pub anonymous_id: Option<String>,
    pub sent_at: DateTime<Utc>,
    pub context: Context,
    #[serde(rename = "type")]
    pub typ: String,
    pub event: Option<String>,
    #[serde(rename = "traits")]
    pub user_properties: Option<HashMap<String, PropValue>>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Type {
    Track,
    Identify,
    Page,
    Screen,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrackRequest {
    pub user_id: Option<String>,
    pub anonymous_id: Option<String>,
    pub sent_at: DateTime<Utc>,
    pub context: Context,
    #[serde(rename = "type")]
    pub typ: Type,
    pub event: Option<String>,
    pub properties: Option<HashMap<String, PropValue>>,
    pub user_properties: Option<HashMap<String, PropValue>>,
}

#[cfg(test)]
mod tests {
    use crate::sources::http::TrackRequest;

    #[test]
    fn test_request() {
        const PAYLOAD: &str = r#"
        {
  "anonymousId": "23adfd82-aa0f-45a7-a756-24f2a7a4c895",
  "context": {
    "library": {
      "name": "analytics.js",
      "version": "2.11.1"
    },
    "page": {
      "path": "/academy/",
      "referrer": "",
      "search": "",
      "title": "Analytics Academy",
      "url": "https://segment.com/academy/"
    },
    "userAgent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.86 Safari/537.36",
    "ip": "108.0.78.21"
  },
  "event": "Course Clicked",
  "integrations": {},
  "messageId": "ajs-f8ca1e4de5024d9430b3928bd8ac6b96",
  "properties": {
    "title": "Intro to Analytics"
  },
  "receivedAt": "2015-12-12T19:11:01.266Z",
  "sentAt": "2015-12-12T19:11:01.169Z",
  "timestamp": "2015-12-12T19:11:01.249Z",
  "type": "track",
  "userId": "AiUGstSDIg",
  "originalTimestamp": "2015-12-12T19:11:01.152Z"
}
    "#;

        let res: TrackRequest = serde_json::from_str(PAYLOAD).unwrap();

        println!("{:?}", res);
    }
}
