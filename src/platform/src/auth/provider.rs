use std::sync::Arc;

use chrono::Duration;
use metadata::accounts::CreateAccountRequest;
use password_hash::PasswordHash;

use super::SignUpRequest;
use crate::auth::password::make_password_hash;
use crate::auth::password::verify_password;
use crate::auth::token::make_access_token;
use crate::auth::token::make_refresh_token;
use crate::auth::token::parse_refresh_token;
use crate::auth::types::TokensResponse;
use crate::PlatformError;
use crate::Result;

#[derive(Clone)]
pub struct Provider {
    accounts: Arc<metadata::accounts::Provider>,
    // accessible from http auth mw
    pub access_token_duration: Duration,
    pub access_token_key: String,
    pub refresh_token_duration: Duration,
    pub refresh_token_key: String,
}

impl Provider {
    pub fn new(
        accounts: Arc<metadata::accounts::Provider>,
        access_token_duration: Duration,
        access_token_key: String,
        refresh_token_duration: Duration,
        refresh_token_key: String,
    ) -> Self {
        Self {
            accounts,
            access_token_duration,
            access_token_key,
            refresh_token_duration,
            refresh_token_key,
        }
    }

    pub async fn sign_up(&self, req: SignUpRequest) -> Result<TokensResponse> {
        let account = self
            .accounts
            .create(CreateAccountRequest {
                created_by: None,
                password_hash: make_password_hash(req.password.as_str())?.to_string(),
                email: req.email,
                first_name: req.first_name,
                last_name: req.last_name,
                role: None,
                organizations: None,
                projects: None,
                teams: None,
            })
            .await?;

        let tokens = self
            .make_tokens(account.id)
            .map_err(|err| PlatformError::Unauthorized(format!("{:?}", err)))?;

        Ok(tokens)
    }

    pub async fn log_in(&self, email: &str, password: &str) -> Result<TokensResponse> {
        let account = self
            .accounts
            .get_by_email(email)
            .await
            .map_err(|err| PlatformError::Unauthorized(format!("{:?}", err)))?;

        verify_password(password, PasswordHash::new(account.password_hash.as_str())?)
            .map_err(|err| PlatformError::Unauthorized(format!("{:?}", err)))?;

        let tokens = self
            .make_tokens(account.id)
            .map_err(|err| PlatformError::Unauthorized(format!("{:?}", err)))?;

        Ok(tokens)
    }

    pub async fn refresh_token(&self, refresh_token: &str) -> Result<TokensResponse> {
        let refresh_claims = parse_refresh_token(refresh_token, self.refresh_token_key.as_str())
            .map_err(|err| PlatformError::Unauthorized(format!("{:?}", err)))?;
        let tokens = self
            .make_tokens(refresh_claims.account_id)
            .map_err(|err| PlatformError::Unauthorized(format!("{:?}", err)))?;

        Ok(tokens)
    }

    fn make_tokens(&self, account_id: u64) -> Result<TokensResponse> {
        Ok(TokensResponse {
            access_token: make_access_token(
                account_id,
                self.access_token_duration,
                self.access_token_key.as_str(),
            )?,
            refresh_token: make_refresh_token(
                account_id,
                self.refresh_token_duration,
                self.refresh_token_key.as_str(),
            )?,
        })
    }
}
