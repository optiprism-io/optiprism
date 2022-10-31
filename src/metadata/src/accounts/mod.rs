pub mod provider_impl;

use async_trait::async_trait;
use chrono::DateTime;
use chrono::Utc;
use common::rbac::OrganizationRole;
use common::rbac::ProjectRole;
use common::rbac::Role;
use common::types::OptionalProperty;
pub use provider_impl::ProviderImpl;
use serde::Deserialize;
use serde::Serialize;

use crate::metadata::ListResponse;
use crate::Result;

#[async_trait]
pub trait Provider: Sync + Send {
    async fn create(&self, req: CreateAccountRequest) -> Result<Account>;
    async fn get_by_id(&self, id: u64) -> Result<Account>;
    async fn get_by_email(&self, email: &str) -> Result<Account>;
    async fn list(&self) -> Result<ListResponse<Account>>;
    async fn update(&self, account_id: u64, req: UpdateAccountRequest) -> Result<Account>;
    async fn delete(&self, id: u64) -> Result<Account>;
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
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

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct CreateAccountRequest {
    pub created_by: Option<u64>,
    pub password_hash: String,
    pub email: String,
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub role: Option<Role>,
    pub organizations: Option<Vec<(u64, OrganizationRole)>>,
    pub projects: Option<Vec<(u64, ProjectRole)>>,
    pub teams: Option<Vec<(u64, Role)>>,
}

impl CreateAccountRequest {
    pub fn into_account(self, id: u64, created_at: DateTime<Utc>) -> Account {
        Account {
            id,
            created_at,
            created_by: self.created_by,
            updated_at: None,
            updated_by: None,
            password_hash: self.password_hash,
            email: self.email,
            first_name: self.first_name,
            last_name: self.last_name,
            role: self.role,
            organizations: self.organizations,
            projects: self.projects,
            teams: self.teams,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct UpdateAccountRequest {
    pub updated_by: u64,
    pub password: OptionalProperty<String>,
    pub email: OptionalProperty<String>,
    pub first_name: OptionalProperty<Option<String>>,
    pub last_name: OptionalProperty<Option<String>>,
    pub role: OptionalProperty<Option<Role>>,
    pub organizations: OptionalProperty<Option<Vec<(u64, OrganizationRole)>>>,
    pub projects: OptionalProperty<Option<Vec<(u64, ProjectRole)>>>,
    pub teams: OptionalProperty<Option<Vec<(u64, Role)>>>,
}
