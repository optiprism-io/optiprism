use chrono::{DateTime, Utc};
use common::rbac::{Permission, Role, Scope};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Serialize, Deserialize)]
pub struct Account {
    pub id: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
    pub admin: bool,
    pub salt: String,
    pub password: String,
    pub organization_id: u64,
    pub email: String,
    pub roles: Option<HashMap<Scope, Role>>,
    pub permissions: Option<HashMap<Scope, Vec<Permission>>>,
    pub first_name: Option<String>,
    pub middle_name: Option<String>,
    pub last_name: Option<String>,
    // TODO: add fields
}

#[derive(Serialize, Deserialize)]
pub struct CreateRequest {
    pub admin: bool,
    pub salt: String,
    pub password: String,
    pub organization_id: u64,
    pub email: String,
    pub roles: Option<HashMap<Scope, Role>>,
    pub permissions: Option<HashMap<Scope, Vec<Permission>>>,
    pub first_name: Option<String>,
    pub middle_name: Option<String>,
    pub last_name: Option<String>,
    // TODO: add fields
}

#[derive(Serialize, Deserialize)]
pub struct UpdateRequest {
    pub id: u64,
    pub first_name: Option<String>,
    pub middle_name: Option<String>,
    pub last_name: Option<String>,
    // TODO: add fields
}

#[derive(Serialize, Deserialize)]
pub struct ListRequest {
    pub limit: u64,
    pub offset: u64,
    // TODO: add fields
}
