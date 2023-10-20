use serde::Deserialize;
use serde::Serialize;

pub mod http;

#[derive(Clone, Deserialize, Debug)]
#[serde(tag = "type", rename_all = "camelCase")]
enum Request {
    Track(http::TrackRequest),
}
