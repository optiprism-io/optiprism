use super::{
    account,
    error::{Result, ERR_AUTH_LOG_IN_INVALID_PASSWORD, ERR_AUTH_LOG_IN_USER_NOT_FOUND},
    organization,
};
use chrono::{Duration, Utc};
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use serde::{Deserialize, Serialize};
use sha3::{Digest, Sha3_256};
use std::{env::var, ops::Add, sync::Arc};

lazy_static::lazy_static! {
    static ref COMMON_SALT: String = var("FNP_COMMON_SALT").unwrap();
    static ref EMAIL_TOKEN_KEY: String = var("FNP_EMAIL_TOKEN_KEY").unwrap();
    static ref ACCESS_TOKEN_KEY: String = var("FNP_ACCESS_TOKEN_KEY").unwrap();
    static ref REFRESH_TOKEN_KEY: String = var("FNP_REFRESH_TOKEN_KEY").unwrap();

    static ref ACCESS_TOKEN_DURATION: Duration = Duration::hours(1);
    static ref REFRESH_TOKEN_DURATION: Duration = Duration::days(30);
}

#[derive(Deserialize)]
struct LogInRequest {
    email: String,
    password: String,
}

#[derive(Deserialize)]
struct SignUpRequest {
    organization_name: String,
    email: String,
    username: String,
    password: String,
}

#[derive(Deserialize)]
struct RefreshRequest {
    refresh_token: String,
}

#[derive(Deserialize)]
struct RecoverRequest {
    email: String,
}

#[derive(Serialize)]
struct TokensResponse {
    access_token: String,
    refresh_token: String,
}

#[derive(Serialize, Deserialize)]
struct JWTClaims {
    organization_id: u64,
    account_id: u64,
    exp: i64,
}

struct Provider {
    account_provider: Arc<account::Provider>,
    organization_provider: Arc<organization::Provider>,
}

impl Provider {
    async fn sign_up(&self, request: SignUpRequest) -> Result<TokensResponse> {
        let salt = make_salt();
        let password = make_password_hash(&request.password, &salt);
        // TODO: create org
        let account = self
            .account_service
            .create(account::Account {
                id: 0,
                salt,
                password,
                roles: vec![account::Role::Owner],
                email: request.email,
                username: request.username,
            })
            .await?;
        make_token_response(user.id)
    }

    fn log_in(&self, request: LogInRequest) -> Result<TokensResponse> {
        let user = match self.user_service.find_by_email(request.email) {
            Some(user) => user,
            None => return Err(ERR_AUTH_LOG_IN_USER_NOT_FOUND.into()),
        };
        if !is_valid_password(&request.password, &user.salt, &user.password) {
            return Err(ERR_AUTH_LOG_IN_INVALID_PASSWORD.into());
        }
        make_token_response(user.id)
    }

    fn new(account_provider: Arc<account::Provider>) -> Self {
        Self { account_provider }
    }
}

fn make_token_response(organization_id: u64, account_id: u64) -> Result<TokensResponse> {
    Ok(TokensResponse {
        access_token: make_jwt_token(
            JWTClaims {
                organization_id,
                account_id,
                exp: Utc::now().add(ACCESS_TOKEN_DURATION.clone()).timestamp(),
            },
            ACCESS_TOKEN_KEY.as_bytes(),
        )?,
        refresh_token: make_jwt_token(
            JWTClaims {
                organization_id,
                account_id,
                exp: Utc::now().add(REFRESH_TOKEN_DURATION.clone()).timestamp(),
            },
            REFRESH_TOKEN_KEY.as_bytes(),
        )?,
    })
}

fn is_valid_password(password: &str, salt: &str, password_hash: &str) -> bool {
    password_hash == make_password_hash(password, salt)
}

fn make_salt() -> String {
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

fn make_password_hash(password: &str, salt: &str) -> String {
    let mut hasher = Sha3_256::new();
    hasher.update(vec![password, salt, &COMMON_SALT].concat());
    hex::encode(hasher.finalize())
}

fn make_jwt_token<T: Serialize>(claims: T, key: &[u8]) -> Result<String> {
    let mut header = jsonwebtoken::Header::default();
    header.alg = jsonwebtoken::Algorithm::HS512;
    Ok(jsonwebtoken::encode(
        &header,
        &claims,
        &jsonwebtoken::EncodingKey::from_secret(key),
    )?)
}

pub fn parse_jwt_access_token(value: &str) -> Result<JWTClaims> {
    let token = jsonwebtoken::decode(
        &value,
        &jsonwebtoken::DecodingKey::from_secret(ACCESS_TOKEN_KEY.as_bytes()),
        &jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::HS512),
    )?;
    Ok(token.claims)
}
