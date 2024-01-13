use std::sync::Arc;

use axum::async_trait;
use chrono::Duration;
use metadata::accounts::Accounts;
use metadata::accounts::CreateAccountRequest;
use password_hash::PasswordHash;

use super::password::make_password_hash;
use super::password::verify_password;
use super::token::make_access_token;
use super::token::make_refresh_token;
use super::token::parse_refresh_token;
use super::SignUpRequest;
use super::UpdateEmailRequest;
use super::UpdateNameRequest;
use super::UpdatePasswordRequest;
use crate::accounts::Account;
use crate::auth::Config;
use crate::auth::LogInRequest;
use crate::auth::Provider;
use crate::auth::TokensResponse;
use crate::error::AuthError;
use crate::Context;
use crate::Result;

#[derive(Clone)]
pub struct ProviderImpl {
    accounts: Arc<Accounts>,
    access_token_duration: Duration,
    access_token_key: String,
    refresh_token_duration: Duration,
    refresh_token_key: String,
}

impl ProviderImpl {
    pub fn new(accounts: Arc<Accounts>, cfg: Config) -> Self {
        Self {
            accounts,
            access_token_duration: cfg.access_token_duration,
            access_token_key: cfg.access_token_key,
            refresh_token_duration: cfg.refresh_token_duration,
            refresh_token_key: cfg.refresh_token_key,
        }
    }

    fn make_tokens(&self, account_id: u64) -> Result<TokensResponse> {
        Ok(TokensResponse {
            access_token: make_access_token(
                account_id,
                self.access_token_duration,
                self.access_token_key.as_str(),
            )
            .map_err(|err| err.wrap_into(AuthError::CantMakeAccessToken))?,
            refresh_token: make_refresh_token(
                account_id,
                self.refresh_token_duration,
                self.refresh_token_key.as_str(),
            )
            .map_err(|err| err.wrap_into(AuthError::CantMakeRefreshToken))?,
        })
    }
}

#[async_trait]
impl Provider for ProviderImpl {
    async fn sign_up(&self, req: SignUpRequest) -> Result<TokensResponse> {
        let password_hash = make_password_hash(req.password.as_str())
            .map_err(|err| err.wrap_into(AuthError::InvalidPasswordHashing))?;

        let account = self.accounts.create(CreateAccountRequest {
            created_by: None,
            password_hash,
            email: req.email,
            first_name: req.first_name,
            last_name: req.last_name,
            role: None,
            organizations: None,
            projects: None,
            teams: None,
        })?;

        let tokens = self.make_tokens(account.id)?;

        Ok(tokens)
    }

    async fn log_in(&self, req: LogInRequest) -> Result<TokensResponse> {
        req.validate()?;

        let account = self
            .accounts
            .get_by_email(&req.email)
            .map_err(|_err| AuthError::InvalidCredentials)?;

        verify_password(
            req.password,
            PasswordHash::new(account.password_hash.as_str())?,
        )
        .map_err(|_err| AuthError::InvalidCredentials)?;
        let tokens = self.make_tokens(account.id)?;

        Ok(tokens)
    }

    async fn refresh_token(&self, refresh_token: &str) -> Result<TokensResponse> {
        let refresh_claims = parse_refresh_token(refresh_token, self.refresh_token_key.as_str())
            .map_err(|err| err.wrap_into(AuthError::InvalidRefreshToken))?;
        let tokens = self.make_tokens(refresh_claims.account_id)?;

        Ok(tokens)
    }

    async fn update_name(&self, _ctx: Context, _req: UpdateNameRequest) -> Result<Account> {
        todo!()
    }

    async fn update_email(&self, _ctx: Context, _req: UpdateEmailRequest) -> Result<Account> {
        todo!()
    }

    async fn update_password(&self, _ctx: Context, _req: UpdatePasswordRequest) -> Result<Account> {
        todo!()
    }
}
