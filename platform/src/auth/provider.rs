use super::{LogInRequest, SignUpRequest, TokensResponse};
use crate::{
    accounts::{CreateRequest as CreateAccountRequest, Provider as AccountProvider},
    Context, Result,
};
use chrono::Utc;
use common::{
    auth::{
        is_valid_password, make_token, AccessClaims, RefreshClaims, ACCESS_TOKEN_DURATION,
        ACCESS_TOKEN_KEY, REFRESH_TOKEN_DURATION, REFRESH_TOKEN_KEY,
    },
    rbac::{Permission, Role, Scope},
};
use metadata::{organizations::CreateRequest as CreateOrganizationRequest, Metadata};
use std::{collections::HashMap, ops::Add, sync::Arc};

pub struct Provider {
    metadata: Arc<Metadata>,
    accounts: Arc<AccountProvider>,
}

impl Provider {
    pub fn new(metadata: Arc<Metadata>, accounts: Arc<AccountProvider>) -> Self {
        Self { metadata, accounts }
    }

    pub async fn sign_up(&self, _ctx: Context, request: SignUpRequest) -> Result<TokensResponse> {
        let organization = self
            .metadata
            .organizations
            .create(CreateOrganizationRequest {
                name: request.organization_name,
            })
            .await?;
        let mut roles = HashMap::new();
        roles.insert(Scope::Organization, Role::Owner);
        let account = self
            .accounts
            .create(
                Context::with_permission(organization.id, Permission::CreateAccount),
                CreateAccountRequest {
                    admin: false,
                    password: request.password,
                    organization_id: organization.id,
                    email: request.email,
                    roles: Some(roles),
                    permissions: None,
                    first_name: request.first_name,
                    middle_name: request.middle_name,
                    last_name: request.last_name,
                },
            )
            .await?;
        make_token_response(
            account.organization_id,
            account.id,
            account.roles,
            account.permissions,
        )
    }

    pub async fn log_in(&self, _ctx: Context, request: LogInRequest) -> Result<TokensResponse> {
        let account = self.metadata.accounts.get_by_email(&request.email).await?;
        if !is_valid_password(&request.password, &account.salt, &account.password) {
            unimplemented!();
        }
        make_token_response(
            account.organization_id,
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
        )?,
        refresh_token: make_token(
            RefreshClaims {
                organization_id,
                account_id,
                exp: Utc::now().add(*REFRESH_TOKEN_DURATION).timestamp(),
            },
            REFRESH_TOKEN_KEY.as_bytes(),
        )?,
    })
}
