use chrono::DateTime;
use chrono::Utc;
use common::types::OptionalProperty;
use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Organization {
    pub id: u64,
    pub created_at: DateTime<Utc>,
    pub created_by: u64,
    pub updated_at: Option<DateTime<Utc>>,
    pub updated_by: Option<u64>,
    pub name: String,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct CreateOrganizationRequest {
    pub created_by: u64,
    pub name: String,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct UpdateOrganizationRequest {
    pub updated_by: u64,
    pub name: OptionalProperty<String>,
}
