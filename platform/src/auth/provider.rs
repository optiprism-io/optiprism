use super::{
    account,
    context::Context,
    error::{Result, ERR_AUTH_LOG_IN_INVALID_PASSWORD},
    organization,
    rbac::{Permission, Role, Scope},
};
use chrono::{Duration, Utc};
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use serde::{Deserialize, Serialize};
use sha3::{Digest, Sha3_256, Sha3_512};
use std::{collections::HashMap, env::var, ops::Add, rc::Rc, sync::Arc};

pub struct Provider {
    organization_provider: Arc<organization::Provider>,
    account_provider: Arc<account::Provider>,
}

impl Provider {
    pub fn new(
        organization_provider: Arc<organization::Provider>,
        account_provider: Arc<account::Provider>,
    ) -> Self {
        Self {
            organization_provider,
            account_provider,
        }
    }

    pub async fn sign_up(
        &self,
        ctx: Rc<Context>,
        request: SignUpRequest,
    ) -> Result<TokensResponse> {
        let org = self
            .organization_provider
            .create(organization::CreateRequest {
                name: request.organization_name,
            })?;
        let mut roles = HashMap::new();
        roles.insert(Scope::Organization, Role::Owner);
        let acc = self.account_provider.create(
            Rc::new(Context::with_permission(org.id, Permission::AccountCreate)),
            account::CreateRequest {
                admin: false,
                password: request.password,
                organization_id: org.id,
                email: request.email,
                roles: Some(roles),
                permissions: None,
                first_name: request.first_name,
                middle_name: request.middle_name,
                last_name: request.last_name,
            },
        )?;
        make_token_response(acc)
    }

    pub fn log_in(&self, ctx: Rc<Context>, request: LogInRequest) -> Result<TokensResponse> {
        let acc = self.account_provider.get_by_email(ctx, request.email)?;
        if !is_valid_password(&request.password, &acc.salt, &acc.password) {
            return Err(ERR_AUTH_LOG_IN_INVALID_PASSWORD.into());
        }
        make_token_response(acc)
    }
}

fn make_token_response(acc: account::Account) -> Result<TokensResponse> {
    Ok(TokensResponse {
        access_token: make_token(
            AccessClaims {
                exp: Utc::now().add(*ACCESS_TOKEN_DURATION).timestamp(),
                organization_id: acc.organization_id,
                account_id: acc.id,
                roles: acc.roles,
                permissions: acc.permissions,
            },
            ACCESS_TOKEN_KEY.as_bytes(),
        )?,
        refresh_token: make_token(
            RefreshClaims {
                organization_id: acc.organization_id,
                account_id: acc.id,
                exp: Utc::now().add(*REFRESH_TOKEN_DURATION).timestamp(),
            },
            REFRESH_TOKEN_KEY.as_bytes(),
        )?,
    })
}

fn make_token<T: Serialize>(claims: T, key: &[u8]) -> Result<String> {
    let header = jsonwebtoken::Header {
        alg: jsonwebtoken::Algorithm::HS512,
        ..Default::default()
    };
    Ok(jsonwebtoken::encode(
        &header,
        &claims,
        &jsonwebtoken::EncodingKey::from_secret(key),
    )?)
}

pub fn parse_access_token(value: &str) -> Result<AccessClaims> {
    let token = jsonwebtoken::decode(
        value,
        &jsonwebtoken::DecodingKey::from_secret(ACCESS_TOKEN_KEY.as_bytes()),
        &jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::HS512),
    )?;
    Ok(token.claims)
}

pub fn parse_refresh_token(value: &str) -> Result<RefreshClaims> {
    let token = jsonwebtoken::decode(
        value,
        &jsonwebtoken::DecodingKey::from_secret(REFRESH_TOKEN_KEY.as_bytes()),
        &jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::HS512),
    )?;
    Ok(token.claims)
}

pub fn is_valid_password(password: &str, salt: &str, password_hash: &str) -> bool {
    password_hash == make_password_hash(password, salt)
}

pub fn make_salt() -> String {
    let rand_part: String = thread_rng()
        .sample_iter(&Alphanumeric)
        .take(64)
        .map(char::from)
        .collect();
    let mut salt: String = Utc::now().to_rfc3339();
    salt.push_str(&rand_part);
    let mut hasher = Sha3_256::new();
    hasher.update(salt);
    hex::encode(hasher.finalize())
}

pub fn make_password_hash(password: &str, salt: &str) -> String {
    let mut hasher = Sha3_512::new();
    hasher.update(vec![password, salt, &COMMON_SALT].concat());
    hex::encode(hasher.finalize())
}
