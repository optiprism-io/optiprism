use metadata::events::Status;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateEventRequest {
    pub tags: Option<Vec<String>>,
    pub name: String,
    pub display_name: Option<String>,
    pub description: Option<String>,
    pub status: Status,
    pub is_system: bool,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateEventRequest {
    pub tags: Option<Vec<String>>,
    pub display_name: Option<String>,
    pub description: Option<String>,
    pub status: Status,
}
