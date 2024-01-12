use serde::Deserialize;

pub mod http;

#[derive(Clone, Deserialize, Debug)]
#[serde(tag = "type", rename_all = "camelCase")]
enum Request {
    Track(http::TrackRequest),
}
