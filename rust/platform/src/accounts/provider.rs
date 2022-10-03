use crate::accounts::types::{Account, CreateAccountRequest, UpdateAccountRequest};
use crate::{Context, Result};
use common::rbac::Permission;
use metadata::accounts;
use metadata::metadata::ListResponse;
use std::sync::Arc;
use crate::auth::password::make_password_hash;

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
        Ok(ListResponse {
            data: resp
                .data
                .iter()
                .map(|v| v.to_owned().try_into())
                .collect::<Result<_>>()?,
            meta: resp.meta,
        })
    }

    pub async fn update(
        &self,
        ctx: Context,
        account_id: u64,
        req: UpdateAccountRequest,
    ) -> Result<Account> {
        ctx.check_permission(Permission::ManageAccounts)?;

        let mut md_req = metadata::accounts::UpdateAccountRequest::default();
        let _ = md_req.updated_by = ctx.account_id.unwrap();
        if let Some(password) = req.password {
            md_req
                .password
                .insert(make_password_hash(password.as_str())?.to_string());
        }
        if let Some(email) = req.email {
            md_req.email.insert(email);
        }
        if let Some(first_name) = req.first_name {
            md_req.first_name.insert(first_name);
        }
        if let Some(last_name) = req.last_name {
            md_req.last_name.insert(last_name);
        }
        if let Some(role) = req.role {
            md_req.role.insert(role);
        }
        if let Some(organizations) = req.organizations {
            md_req.organizations.insert(organizations);
        }
        if let Some(projects) = req.projects {
            md_req.projects.insert(projects);
        }
        if let Some(teams) = req.teams {
            md_req.teams.insert(teams);
        }
        let account = self.prov.update(account_id, md_req).await?;

        account.try_into()
    }

    pub async fn delete(&self, ctx: Context, id: u64) -> Result<Account> {
        ctx.check_permission(Permission::ManageAccounts)?;

        self.prov.delete(id).await?.try_into()
    }
}
