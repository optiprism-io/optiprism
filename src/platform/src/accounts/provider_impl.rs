use std::sync::Arc;

use axum::async_trait;
use common::rbac::Permission;
use common::types::OptionalProperty;
use metadata::accounts;

use super::Account;
use super::CreateAccountRequest;
use super::Provider;
use super::UpdateAccountRequest;
use crate::auth::password::make_password_hash;
use crate::auth::UpdateEmailRequest;
use crate::auth::UpdateNameRequest;
use crate::auth::UpdatePasswordRequest;
use crate::Context;
use crate::ListResponse;
use crate::Result;

pub struct ProviderImpl {
    prov: Arc<dyn accounts::Provider>,
}

impl ProviderImpl {
    pub fn new(prov: Arc<dyn accounts::Provider>) -> Self {
        Self { prov }
    }
}
#[async_trait]
impl Provider for ProviderImpl {
    async fn create(&self, ctx: Context, req: CreateAccountRequest) -> Result<Account> {
        ctx.check_permission(Permission::ManageAccounts)?;

        let md_req = metadata::accounts::CreateAccountRequest {
            created_by: ctx.account_id,
            password_hash: make_password_hash(req.password.as_str())?,
            email: req.email,
            first_name: req.first_name,
            last_name: req.last_name,
            role: req.role,
            organizations: req.organizations,
            projects: req.projects,
            teams: req.teams,
        };

        let account = self.prov.create(md_req)?;

        account.try_into()
    }

    async fn get_by_id(&self, ctx: Context, id: u64) -> Result<Account> {
        ctx.check_permission(Permission::ManageAccounts)?;

        self.prov.get_by_id(id)?.try_into()
    }

    async fn list(&self, ctx: Context) -> Result<ListResponse<Account>> {
        ctx.check_permission(Permission::ManageAccounts)?;
        let resp = self.prov.list()?;
        resp.try_into()
    }

    async fn update(
        &self,
        ctx: Context,
        account_id: u64,
        req: UpdateAccountRequest,
    ) -> Result<Account> {
        ctx.check_permission(Permission::ManageAccounts)?;

        let mut md_req = metadata::accounts::UpdateAccountRequest {
            updated_by: ctx.account_id.unwrap(),
            email: req.email,
            first_name: req.first_name,
            last_name: req.last_name,
            role: req.role,
            organizations: req.organizations,
            projects: req.projects,
            teams: req.teams,
            ..Default::default()
        };
        if let OptionalProperty::Some(password) = req.password {
            md_req
                .password
                .insert(make_password_hash(password.as_str())?);
        }

        let account = self.prov.update(account_id, md_req)?;

        account.try_into()
    }

    async fn delete(&self, ctx: Context, id: u64) -> Result<Account> {
        ctx.check_permission(Permission::ManageAccounts)?;

        self.prov.delete(id)?.try_into()
    }
}
