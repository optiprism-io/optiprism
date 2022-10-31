pub mod provider_impl;

use axum::async_trait;
use chrono::DateTime;
use chrono::Utc;
use common::rbac::OrganizationRole;
use common::rbac::ProjectRole;
use common::rbac::Role;
use common::types::OptionalProperty;
pub use provider_impl::ProviderImpl;
use serde::Deserialize;
use serde::Serialize;

use crate::Context;
use crate::ListResponse;
use crate::PlatformError;
use crate::Result;

#[async_trait]
pub trait Provider: Sync + Send {
    async fn create(&self, ctx: Context, req: CreateAccountRequest) -> Result<Account>;
    async fn get_by_id(&self, ctx: Context, id: u64) -> Result<Account>;
    async fn list(&self, ctx: Context) -> Result<ListResponse<Account>>;
    async fn update(
        &self,
        ctx: Context,
        account_id: u64,
        req: UpdateAccountRequest,
    ) -> Result<Account>;
    async fn delete(&self, ctx: Context, id: u64) -> Result<Account>;
}

#[derive(Serialize, Deserialize, Clone, Debug)]
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

    fn try_into(self) -> std::result::Result<metadata::accounts::Account, Self::Error> {
        Ok(metadata::accounts::Account {
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
            teams: self.teams,
        })
    }
}

impl TryInto<Account> for metadata::accounts::Account {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<Account, Self::Error> {
        Ok(Account {
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
            teams: self.teams,
        })
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
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

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
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
