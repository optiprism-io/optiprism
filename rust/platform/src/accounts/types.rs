use chrono::{DateTime, Utc};
use common::rbac::{OrganizationRole, Permission, ProjectRole, Role};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use common::types::OptionalProperty;
use crate::PlatformError;

#[derive(Serialize, Deserialize, Clone)]
pub struct Account {
    pub id: u64,
    pub created_at: DateTime<Utc>,
    pub created_by: Option<u64>,
    pub updated_at: Option<DateTime<Utc>>,
    pub updated_by: Option<u64>,
    pub password_hash: String,
    pub email: String,
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub role: Option<Role>,
    pub organizations: Option<Vec<(u64, OrganizationRole)>>,
    pub projects: Option<Vec<(u64, ProjectRole)>>,
    pub teams: Option<Vec<(u64, Role)>>,
}

impl TryInto<metadata::accounts::Account> for Account {
    type Error = PlatformError;

    fn try_into(self) -> Result<metadata::accounts::Account, Self::Error> {
        Ok(metadata::accounts::Account{
            id: self.id,
            created_at: self.created_at,
            created_by: self.created_by,
            updated_at: self.updated_at,
            updated_by: self.updated_by,
            password_hash: self.password_hash,
            email: self.email,
            first_name: self.first_name,
            last_name: self.last_name,
            role: self.role,
            organizations: self.organizations,
            projects: self.projects,
            teams: self.teams
        })
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CreateAccountRequest {
    pub password: String,
    pub email: String,
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub role: Option<Role>,
    pub organizations: Option<Vec<(u64, OrganizationRole)>>,
    pub projects: Option<Vec<(u64, ProjectRole)>>,
    pub teams: Option<Vec<(u64, Role)>>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct UpdateAccountRequest {
    pub salt: OptionalProperty<String>,
    pub password: OptionalProperty<String>,
    pub email: OptionalProperty<String>,
    pub first_name: OptionalProperty<Option<String>>,
    pub last_name: OptionalProperty<Option<String>>,
    pub role: OptionalProperty<Option<Role>>,
    pub organizations: OptionalProperty<Option<Vec<(u64, OrganizationRole)>>>,
    pub projects: OptionalProperty<Option<Vec<(u64, ProjectRole)>>>,
    pub teams: OptionalProperty<Option<Vec<(u64, Role)>>>,
}