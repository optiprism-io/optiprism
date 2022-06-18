use metadata::events::{Scope, Status};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct CreateRequest {
    pub project_id: u64,
    pub tags: Option<Vec<String>>,
    pub name: String,
    pub display_name: Option<String>,
    pub description: Option<String>,
    pub status: Status,
    pub scope: Scope,
    pub properties: Option<Vec<u64>>,
    pub custom_properties: Option<Vec<u64>>,
}

#[derive(Serialize, Deserialize)]
pub struct UpdateRequest {
    pub id: u64,
    pub project_id: u64,
    pub tags: Option<Vec<String>>,
    pub name: String,
    pub display_name: Option<String>,
    pub description: Option<String>,
    pub status: Status,
    pub scope: Scope,
    pub properties: Option<Vec<u64>>,
    pub custom_properties: Option<Vec<u64>>,
}
