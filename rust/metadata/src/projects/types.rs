use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use common::types::OptionalProperty;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Project {
    pub id: u64,
    pub created_at: DateTime<Utc>,
    pub created_by: u64,
    pub updated_at: Option<DateTime<Utc>>,
    pub updated_by: Option<u64>,
    pub organization_id: u64,
    pub name: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CreateProjectRequest {
    pub created_by: u64,
    pub organization_id: u64,
    pub name: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct UpdateProjectRequest {
    pub updated_by: u64,
    pub name: OptionalProperty<String>,
}