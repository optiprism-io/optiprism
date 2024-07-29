use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use common::rbac::OrganizationRole;
use common::rbac::Permission;
use common::rbac::ProjectRole;
use common::rbac::Role;
use common::types::OptionalProperty;
use metadata::accounts::Accounts as MDAccounts;
use serde::Deserialize;
use serde::Serialize;

use crate::auth::password::make_password_hash;
use crate::auth::provider::Profile;
use crate::Context;
use crate::ListResponse;
use crate::PlatformError;
use crate::Result;

pub struct Accounts {
    prov: Arc<MDAccounts>,
}

impl Accounts {
    pub fn new(prov: Arc<MDAccounts>) -> Self {
        Self { prov }
    }

    pub async fn create(&self, ctx: Context, req: CreateAccountRequest) -> Result<Account> {
        ctx.check_permission(Permission::ManageAccounts)?;

        let md_req = metadata::accounts::CreateAccountRequest {
            created_by: ctx.account_id,
            password_hash: make_password_hash(req.password.as_str())?,
            email: req.email,
            name: req.name,
            force_update_password: req.force_update_password,
            force_update_email: req.force_update_email,
            role: req.role,
            organizations: req.organizations,
            projects: req.projects,
            teams: req.teams,
        };

        let account = self.prov.create(md_req)?;

        Ok(account.into())
    }

    pub async fn get_by_id(&self, ctx: Context, id: u64) -> Result<Account> {
        ctx.check_permission(Permission::ManageAccounts)?;

        Ok(self.prov.get_by_id(id)?.into())
    }

    pub async fn list(&self, ctx: Context) -> Result<ListResponse<Account>> {
        ctx.check_permission(Permission::ManageAccounts)?;
        let resp = self.prov.list()?;
        Ok(resp.into())
    }

    pub async fn update(
        &self,
        ctx: Context,
        account_id: u64,
        req: UpdateAccountRequest,
    ) -> Result<Account> {
        ctx.check_permission(Permission::ManageAccounts)?;

        let mut md_req = metadata::accounts::UpdateAccountRequest {
            updated_by: ctx.account_id,
            email: req.email,
            name: req.name,
            role: req.role,
            organizations: req.organizations,
            projects: req.projects,
            teams: req.teams,
            ..Default::default()
        };
        if let OptionalProperty::Some(password) = req.password {
            md_req
                .password_hash
                .insert(make_password_hash(password.as_str())?);
        }

        let account = self.prov.update(account_id, md_req)?;

        Ok(account.into())
    }

    pub async fn delete(&self, ctx: Context, id: u64) -> Result<Account> {
        ctx.check_permission(Permission::ManageAccounts)?;

        Ok(self.prov.delete(id)?.into())
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Account {
    pub id: u64,
    pub created_at: DateTime<Utc>,
    pub created_by: u64,
    pub updated_at: Option<DateTime<Utc>>,
    pub updated_by: Option<u64>,
    pub email: String,
    pub name: Option<String>,
    pub force_update_password: bool,
    pub force_update_email: bool,
    pub role: Option<Role>,
    pub organizations: Option<Vec<(u64, OrganizationRole)>>,
    pub projects: Option<Vec<(u64, ProjectRole)>>,
    pub teams: Option<Vec<(u64, Role)>>,
}

impl Into<Account> for metadata::accounts::Account {
    fn into(self) -> Account {
        Account {
            id: self.id,
            created_at: self.created_at,
            created_by: self.created_by,
            updated_at: self.updated_at,
            updated_by: self.updated_by,
            email: self.email,
            name: self.name,
            force_update_password: self.force_update_password,
            force_update_email: self.force_update_email,
            role: self.role,
            organizations: self.organizations,
            projects: self.projects,
            teams: self.teams,
        }
    }
}

impl Into<Profile> for Account {
    fn into(self) -> Profile {
        Profile {
            email: self.email,
            name: self.name,
            force_update_password: self.force_update_password,
            force_update_email: self.force_update_email,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CreateAccountRequest {
    pub password: String,
    pub email: String,
    pub name: Option<String>,
    pub force_update_password: bool,
    pub force_update_email: bool,
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
    pub name: OptionalProperty<Option<String>>,
    pub force_update_password: OptionalProperty<bool>,
    pub role: OptionalProperty<Option<Role>>,
    pub organizations: OptionalProperty<Option<Vec<(u64, OrganizationRole)>>>,
    pub projects: OptionalProperty<Option<Vec<(u64, ProjectRole)>>>,
    pub teams: OptionalProperty<Option<Vec<(u64, Role)>>>,
}
