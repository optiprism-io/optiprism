use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct Project {
    pub id: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
    pub organization_id: u64,
    pub name: String,
    // TODO: add fields
}

#[derive(Serialize, Deserialize)]
pub struct CreateRequest {
    pub organization_id: u64,
    pub name: String,
    // TODO: add fields
}

#[derive(Serialize, Deserialize)]
pub struct UpdateRequest {
    pub id: u64,
    pub organization_id: u64,
    pub name: Option<String>,
    // TODO: add fields
}

#[derive(Serialize, Deserialize)]
pub struct ListRequest {
    pub organization_id: u64,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
    // TODO: add fields
}
