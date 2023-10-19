use std::collections::HashMap;
use std::net::Ipv4Addr;

use chrono::DateTime;
use chrono::Utc;
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::sources::http::Context;
use crate::sources::http::PropValue;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
enum Type {
    Track,
    Identify,
    Page,
    Screen,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrackRequest {
    pub user_id: Option<String>,
    pub anonymous_id: Option<String>,
    pub sent_at: DateTime<Utc>,
    pub context: Context,
    #[serde(rename = "type")]
    pub typ: Type,
    pub event: Option<String>,
    pub properties: HashMap<String, PropValue>,
    pub user_properties: HashMap<String, PropValue>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrackResponse {}

#[cfg(test)]
mod tests {
    use crate::sources::http::track::TrackRequest;

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
