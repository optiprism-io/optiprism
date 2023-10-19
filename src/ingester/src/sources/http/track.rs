use std::collections::HashMap;
use std::net::Ipv4Addr;

use chrono::DateTime;
use chrono::Utc;
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::sources::http::PropValue;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrackRequest {
    pub user_id: Option<String>,
    pub sent_at: Option<DateTime<Utc>>,
    pub context: Option<Context>,
    pub event: String,
    pub properties: HashMap<String, PropValue>,
    pub user_properties: HashMap<String, PropValue>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Context {
    pub library: Option<Library>,
    pub page: Option<Page>,
    pub user_agent: Option<String>,
    pub ip: Option<Ipv4Addr>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Library {
    pub name: String,
    pub version: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Page {
    pub path: Option<String>,
    pub referrer: Option<String>,
    pub search: Option<String>,
    pub title: Option<String>,
    pub url: Option<String>,
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
          "userId": "qwe123",
          "sentAt": "2015-12-12T19:11:01.169Z",
          "context": {
            "library": {
              "name": "analytics.js",
              "version": "2.11.1"
            },
            "page": {
              "path": "/search/",
              "referrer": "https://google.com",
              "search": "tv",
              "title": "Search Products",
              "url": "https://myshop.com/search/"
            },
            "userAgent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.86 Safari/537.36",
            "ip": "101.10.8.21"
          },
          "event": "Buy Product",
          "properties": {
            "Product Name": "Samsung TV",
            "Product Size": 60,
            "HDR": true,
            "Number": 899,
            "Price": 3.14159265359
          },
          "userProperties": {
            "Email": "sdf@asdf.com",
            "Time": "2015-12-12T19:11:01.169Z"
          }
        }
    "#;

        let res: TrackRequest = serde_json::from_str(PAYLOAD).unwrap();

        println!("{:?}", res);
    }
}
