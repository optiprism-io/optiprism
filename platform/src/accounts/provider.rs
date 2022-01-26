use super::CreateRequest;
use crate::{Context, Result};
use common::{
    auth::{make_password_hash, make_salt},
    rbac::Permission,
};
use metadata::{
    accounts::{Account, CreateRequest as CreateAccountRequest},
    Metadata,
};
use std::sync::Arc;

pub struct Provider {
    metadata: Arc<Metadata>,
}

impl Provider {
    pub fn new(metadata: Arc<Metadata>) -> Self {
        Self { metadata }
    }

    pub async fn create(&self, ctx: Context, request: CreateRequest) -> Result<Account> {
        if !ctx.is_permitted(request.organization_id, 0, Permission::CreateAccount) {
            unimplemented!()
        }
        let salt = make_salt();
        let password = make_password_hash(&request.password, &salt);
        let account = self
            .metadata
            .accounts
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
