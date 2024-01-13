pub mod password;
pub mod provider_impl;
pub mod token;

use axum::async_trait;
use chrono::Duration;
pub use provider_impl::ProviderImpl;
use serde::Deserialize;
use serde::Serialize;
use validator::validate_email;

use crate::accounts::Account;
use crate::error::ValidationError;
use crate::Context;
use crate::Result;

#[async_trait]
pub trait Provider: Sync + Send {
    async fn sign_up(&self, req: SignUpRequest) -> Result<TokensResponse>;
    async fn log_in(&self, req: LogInRequest) -> Result<TokensResponse>;
    async fn refresh_token(&self, refresh_token: &str) -> Result<TokensResponse>;
    async fn update_name(&self, ctx: Context, req: UpdateNameRequest) -> Result<Account>;
    async fn update_email(&self, ctx: Context, req: UpdateEmailRequest) -> Result<Account>;
    async fn update_password(&self, ctx: Context, req: UpdatePasswordRequest) -> Result<Account>;
}

#[derive(Clone)]
pub struct Config {
    pub access_token_duration: Duration,
    pub access_token_key: String,
    pub refresh_token_duration: Duration,
    pub refresh_token_key: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SignUpRequest {
    pub email: String,
    pub password: String,
    pub password_repeat: String,
    pub first_name: Option<String>,
    pub last_name: Option<String>,
}

impl SignUpRequest {
    pub fn validate(&self) -> Result<()> {
        let mut res_err = ValidationError::new();
        if !validate_email(&self.email) {
            res_err.push_invalid("email")
        }

        match zxcvbn::zxcvbn(&self.password, &[&self.email]) {
            Ok(ent) if ent.score() < 3 => res_err.push("password", "password is too simple"),
            Err(err) => res_err.push("password", err.to_string()),
            _ => {}
        }

        res_err.result()
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LogInRequest {
    pub email: String,
    pub password: String,
}

impl LogInRequest {
    pub fn validate(&self) -> Result<()> {
        let mut err = ValidationError::new();
        if !validate_email(&self.email) {
            err.push_invalid("email")
        }

        err.result()
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RecoverPasswordRequest {
    pub email: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TokensResponse {
    pub access_token: String,
    pub refresh_token: String,
}

pub struct UpdateNameRequest {
    pub name: String,
    pub current_password: String,
}

pub struct UpdateEmailRequest {
    pub email: String,
    pub current_password: String,
}

pub struct UpdatePasswordRequest {
    pub current_password: String,
    pub new_password: String,
}
