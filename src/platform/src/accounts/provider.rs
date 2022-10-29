use std::sync::Arc;

use common::rbac::Permission;
use common::types::OptionalProperty;
use metadata::accounts;

use crate::accounts::types::Account;
use crate::accounts::types::CreateAccountRequest;
use crate::accounts::types::UpdateAccountRequest;
use crate::auth::password::make_password_hash;
use crate::types::ListResponse;
use crate::Context;
use crate::Result;

pub struct Provider {
    prov: Arc<accounts::Provider>,
}

impl Provider {
    pub fn new(prov: Arc<accounts::Provider>) -> Self {
        Self { prov }
    }

    pub async fn create(&self, ctx: Context, req: CreateAccountRequest) -> Result<Account> {
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

        let account = self.prov.create(md_req).await?;

        account.try_into()
    }

    pub async fn get_by_id(&self, ctx: Context, id: u64) -> Result<Account> {
        ctx.check_permission(Permission::ManageAccounts)?;

        self.prov.get_by_id(id).await?.try_into()
    }

    pub async fn list(&self, ctx: Context) -> Result<ListResponse<Account>> {
        ctx.check_permission(Permission::ManageAccounts)?;
        let resp = self.prov.list().await?;
        resp.try_into()
    }

    pub async fn update(
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

        let account = self.prov.update(account_id, md_req).await?;

        account.try_into()
    }

    pub async fn delete(&self, ctx: Context, id: u64) -> Result<Account> {
        ctx.check_permission(Permission::ManageAccounts)?;

        self.prov.delete(id).await?.try_into()
    }
}
