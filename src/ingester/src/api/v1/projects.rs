use std::collections::HashMap;
use chrono::{DateTime, Utc};
use crate::event::{Context, PropValue};

#[derive(Debug, Deserialize)]
struct EventRequest {
    #[serde(rename = "userId")]
    user_id: String,
    #[serde(rename = "sentAt")]
    sent_at: Option<DateTime<Utc>>,
    context: Option<Context>,
    event: String,
    #[serde(default)]
    properties: HashMap<String, Option<PropValue>>,
    #[serde(rename = "userProperties", default)]
    user_properties: HashMap<String, Option<PropValue>>,
}


#[cfg(test)]
mod tests {
    use super::*;

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
            "Price": 899
          },
          "userProperties": {
            "Email": "sdf@asdf.com",
            "Time": "2015-12-12T19:11:01.169Z"
          }
        }
    "#;

    #[test]
    fn parse() {
        let event: EventRequest = serde_json::from_str(PAYLOAD).unwrap();
        println!("{:#?}", event);
    }
}
