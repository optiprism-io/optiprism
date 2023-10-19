use serde::Deserialize;
use serde::Serialize;

pub mod http;

#[derive(Clone, Deserialize, Debug, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "camelCase")]
enum Request {
    Track(http::TrackRequest),
}
