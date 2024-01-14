use std::sync::Arc;

use chrono::Duration;
use common::types::OptionalProperty;
use metadata::accounts::Accounts;
use metadata::accounts::CreateAccountRequest;
use metadata::accounts::UpdateAccountRequest;
use password_hash::PasswordHash;
use serde::Deserialize;
use serde::Serialize;
use validator::validate_email;

use super::password::make_password_hash;
use super::password::verify_password;
use super::token::make_access_token;
use super::token::make_refresh_token;
use super::token::parse_refresh_token;
use crate::accounts::Account;
use crate::error::AuthError;
use crate::error::ValidationError;
use crate::Context;
use crate::Result;

#[derive(Clone)]
pub struct Auth {
    accounts: Arc<Accounts>,
    access_token_duration: Duration,
    access_token_key: String,
    refresh_token_duration: Duration,
    refresh_token_key: String,
}

impl Auth {
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

    pub async fn sign_up(&self, req: SignUpRequest) -> Result<TokensResponse> {
        let password_hash = make_password_hash(req.password.as_str())
            .map_err(|err| err.wrap_into(AuthError::InvalidPasswordHashing))?;

        let account = self.accounts.create(CreateAccountRequest {
            created_by: None,
            password_hash,
            email: req.email,
            name: req.name,
            role: None,
            organizations: None,
            projects: None,
            teams: None,
        })?;

        let tokens = self.make_tokens(account.id)?;

        Ok(tokens)
    }

    pub async fn log_in(&self, req: LogInRequest) -> Result<TokensResponse> {
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

    pub async fn refresh_token(&self, refresh_token: &str) -> Result<TokensResponse> {
        let refresh_claims = parse_refresh_token(refresh_token, self.refresh_token_key.as_str())
            .map_err(|err| err.wrap_into(AuthError::InvalidRefreshToken))?;
        let tokens = self.make_tokens(refresh_claims.account_id)?;

        Ok(tokens)
    }

    pub async fn get(&self, ctx: Context) -> Result<Account> {
        self.accounts.get_by_id(ctx.account_id.unwrap())?.try_into()
    }

    pub async fn update_name(&self, ctx: Context, req: String) -> Result<()> {
        let md_req = UpdateAccountRequest {
            updated_by: ctx.account_id.unwrap(),
            name: OptionalProperty::Some(Some(req)),
            email: OptionalProperty::None,
            role: OptionalProperty::None,
            organizations: OptionalProperty::None,
            projects: OptionalProperty::None,
            teams: OptionalProperty::None,
            password: OptionalProperty::None,
        };

        self.accounts.update(ctx.account_id.unwrap(), md_req)?;

        Ok(())
    }

    pub async fn update_email(
        &self,
        ctx: Context,
        req: UpdateEmailRequest,
    ) -> Result<TokensResponse> {
        let account = self.accounts.get_by_id(ctx.account_id.unwrap())?;

        verify_password(
            req.current_password,
            PasswordHash::new(account.password_hash.as_str())?,
        )
        .map_err(|_err| AuthError::InvalidCredentials)?;

        let md_req = UpdateAccountRequest {
            updated_by: ctx.account_id.unwrap(),
            name: OptionalProperty::None,
            email: OptionalProperty::Some(req.email),
            role: OptionalProperty::None,
            organizations: OptionalProperty::None,
            projects: OptionalProperty::None,
            teams: OptionalProperty::None,
            password: OptionalProperty::None,
        };

        self.accounts.update(ctx.account_id.unwrap(), md_req)?;

        let tokens = self.make_tokens(account.id)?;

        Ok(tokens)
    }

    pub async fn update_password(
        &self,
        ctx: Context,
        req: UpdatePasswordRequest,
    ) -> Result<TokensResponse> {
        let account = self.accounts.get_by_id(ctx.account_id.unwrap())?;

        verify_password(
            req.current_password,
            PasswordHash::new(account.password_hash.as_str())?,
        )
        .map_err(|_err| AuthError::InvalidCredentials)?;

        let password_hash = make_password_hash(req.new_password.as_str())
            .map_err(|err| err.wrap_into(AuthError::InvalidPasswordHashing))?;

        let md_req = UpdateAccountRequest {
            updated_by: ctx.account_id.unwrap(),
            name: OptionalProperty::None,
            email: OptionalProperty::None,
            role: OptionalProperty::None,
            organizations: OptionalProperty::None,
            projects: OptionalProperty::None,
            teams: OptionalProperty::None,
            password: OptionalProperty::Some(password_hash),
        };

        self.accounts.update(ctx.account_id.unwrap(), md_req)?;

        let tokens = self.make_tokens(account.id)?;

        Ok(tokens)
    }
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
    pub name: Option<String>,
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

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct UpdateNameRequest {
    pub name: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct UpdateEmailRequest {
    pub email: String,
    pub current_password: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct UpdatePasswordRequest {
    pub current_password: String,
    pub new_password: String,
}
