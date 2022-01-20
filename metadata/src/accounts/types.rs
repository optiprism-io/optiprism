use chrono::{DateTime, Utc};
use common::rbac::{Permission, Role, Scope};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Serialize, Deserialize)]
pub struct Account {
    pub id: u64,
    pub created_at: DateTime<Utc>,
    pub created_by: u64,
    pub updated_at: Option<DateTime<Utc>>,
    pub updated_by: Option<u64>,
    pub admin: bool,
    pub salt: String,
    pub password: String,
    pub organization_id: u64,
    pub email: String,
    pub roles: Option<HashMap<Scope, Role>>,
    pub permissions: Option<HashMap<Scope, Vec<Permission>>>,
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    // TODO: add fields
}

#[derive(Serialize, Deserialize, Clone, PartialEq)]
pub struct CreateAccountRequest {
    pub created_by: u64,
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
}

impl CreateAccountRequest {
    pub fn into_account(self, id: u64, created_at: DateTime<Utc>) -> Account {
        Account {
            id,
            created_at,
            created_by: self.created_by,
            updated_at: None,
            updated_by: None,
            admin: self.admin,
            salt: self.salt,
            password: self.password,
            organization_id: self.organization_id,
            email: self.email,
            roles: self.roles,
            permissions: self.permissions,
            first_name: self.first_name,
            last_name: self.last_name,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, PartialEq)]
pub struct UpdateAccountRequest {
    pub id: u64,
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
}

impl UpdateAccountRequest {
    pub fn into_account(self, prev: Account, updated_at: DateTime<Utc>, updated_by: Option<u64>) -> Account {
        Account {
            id: self.id,
            created_at: prev.created_at,
            created_by: prev.created_by,
            updated_at: Some(updated_at),
            updated_by,
            admin: self.admin,
            salt: self.salt,
            password: self.password,
            organization_id: self.organization_id,
            email: self.email,
            roles: self.roles,
            permissions: self.permissions,
            first_name: self.first_name,
            last_name: self.last_name,
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct ListAccountsRequest {
    pub limit: u64,
    pub offset: u64,
    // TODO: add fields
}
