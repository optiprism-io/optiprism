use super::{LogInRequest, SignUpRequest, TokensResponse};
use crate::{Context, PlatformError, Result};
use chrono::Utc;
use common::auth::{make_password_hash, make_salt};
use common::{
    auth::{
        is_valid_password, make_token, AccessClaims, RefreshClaims, ACCESS_TOKEN_DURATION,
        ACCESS_TOKEN_KEY, REFRESH_TOKEN_DURATION, REFRESH_TOKEN_KEY,
    },
    rbac::{Permission, Role, Scope},
};

use metadata::{
    accounts::CreateAccountRequest as CreateAccountRequest,
    organizations::CreateRequest as CreateOrganizationRequest, Metadata,
};
use std::{collections::HashMap, ops::Add, sync::Arc};
use metadata::error::{AccountError, MetadataError};
use crate::auth::auth::is_valid_password;
use crate::error::AuthError;

pub struct Provider {
    md: Arc<Metadata>,
}

impl Provider {
    pub fn new(md: Arc<Metadata>) -> Self {
        Self { md }
    }

    pub async fn sign_up(&self, ctx: Context, request: SignUpRequest) -> Result<TokensResponse> {
        let mut roles = HashMap::new();
        roles.insert(Scope::Organization, Role::Owner);

        let salt = make_salt();
        let password = make_password_hash(&request.password, &salt);
        let account = self
            .md
            .accounts
            .create(CreateAccountRequest {
                created_by: ctx.account_id,
                admin: false,
                salt,
                password,
                organization_id: organization.id,
                email: request.email,
                roles: Some(roles),
                permissions: None,
                first_name: request.first_name,
                middle_name: request.middle_name,
                last_name: request.last_name,
            })
            .await?;
        make_token_response(
            account.organization_id,
            account.id,
            account.roles,
            account.permissions,
        )
    }

    pub async fn log_in(&self, _ctx: Context, request: LogInRequest) -> Result<TokensResponse> {
        let account = match self.md.accounts.get_by_email(&request.email).await {
            Ok(account) =>account,
            Err(err) => return Err(AuthError::InvalidCredentials.into())
        };

        if !is_valid_password(&request.password, &account.salt, &account.password) {
            return Err(AuthError::InvalidCredentials.into());
        }

        make_token_response(
            account.id,
            account.roles,
            account.permissions,
        )
    }
}

fn make_token_response(
    organization_id: u64,
    account_id: u64,
    roles: Option<HashMap<Scope, Role>>,
    permissions: Option<HashMap<Scope, Vec<Permission>>>,
) -> Result<TokensResponse> {
    Ok(TokensResponse {
        access_token: make_token(
            AccessClaims {
                exp: Utc::now().add(*ACCESS_TOKEN_DURATION).timestamp(),
                organization_id,
                account_id,
                roles,
                permissions,
            },
            ACCESS_TOKEN_KEY.as_bytes(),
        )
            .expect("error"),
        refresh_token: make_token(
            RefreshClaims {
                organization_id,
                account_id,
                exp: Utc::now().add(*REFRESH_TOKEN_DURATION).timestamp(),
            },
            REFRESH_TOKEN_KEY.as_bytes(),
        )
            .expect("error"),
    })
}
