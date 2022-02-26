use super::CreateRequest;
use crate::{Context, Result};
use common::{
    auth::{make_password_hash, make_salt},
    rbac::Permission,
};
use metadata::accounts::{
    Account, CreateRequest as CreateAccountRequest, Provider as AccountProvider,
};
use std::sync::Arc;

pub struct Provider {
    prov: Arc<AccountProvider>,
}

impl Provider {
    pub fn new(prov: Arc<AccountProvider>) -> Self {
        Self { prov }
    }

    pub async fn create(&self, ctx: Context, request: CreateRequest) -> Result<Account> {
        ctx.check_permission(request.organization_id, 0, Permission::CreateAccount)?;

        let salt = make_salt();
        let password = make_password_hash(&request.password, &salt);
        let account = self
            .prov
            .create(CreateAccountRequest {
                created_by: ctx.account_id,
                admin: request.admin,
                salt,
                password,
                organization_id: request.organization_id,
                email: request.email,
                roles: request.roles,
                permissions: request.permissions,
                first_name: request.first_name,
                middle_name: request.middle_name,
                last_name: request.last_name,
            })
            .await?;
        Ok(account)
    }
}
