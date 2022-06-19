use metadata::properties::Status;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdatePropertyRequest {
    pub tags: Option<Vec<String>>,
    pub display_name: Option<String>,
    pub description: Option<String>,
    pub status: Status,
}
