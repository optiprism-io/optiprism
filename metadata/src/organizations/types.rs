use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct Organization {
    pub id: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
    pub name: String,
    // TODO: add fields
}

#[derive(Serialize, Deserialize)]
pub struct CreateRequest {
    pub name: String,
    // TODO: add fields
}

#[derive(Serialize, Deserialize)]
pub struct UpdateRequest {
    pub id: u64,
    pub name: Option<String>,
    // TODO: add fields
}

#[derive(Serialize, Deserialize)]
pub struct ListRequest {
    // pub order
    pub limit: Option<u64>,
    pub offset: Option<u64>,
    // TODO: add fields
}
