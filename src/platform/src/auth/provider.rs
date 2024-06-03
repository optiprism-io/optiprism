use std::sync::Arc;

use chrono::Duration;
use common::config::Config;
use common::types::OptionalProperty;
use common::ADMIN_ID;
use common::GROUP_USER_ID;
use metadata::accounts::Accounts;
use metadata::accounts::CreateAccountRequest;
use metadata::accounts::UpdateAccountRequest;
use metadata::error::MetadataError;
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
use crate::Context;
use crate::PlatformError;
use crate::Result;

#[derive(Clone)]
pub struct Auth {
    accounts: Arc<Accounts>,
    cfg: Config,
}

impl Auth {
    pub fn new(accounts: Arc<Accounts>, cfg: Config) -> Self {
        Self { accounts, cfg }
    }

    fn make_tokens(&self, account_id: u64) -> Result<TokensResponse> {
        Ok(TokensResponse {
            access_token: make_access_token(
                account_id,
                1, // todo implement org management
                self.cfg.access_token_duration.clone(),
                "access",
            )
            .map_err(|err| err.wrap_into(AuthError::CantMakeAccessToken))?,
            refresh_token: make_refresh_token(
                account_id,
                self.cfg.refresh_token_duration.clone(),
                "refresh",
            )
            .map_err(|err| err.wrap_into(AuthError::CantMakeRefreshToken))?,
        })
    }

    pub async fn sign_up(&self, req: SignUpRequest) -> Result<TokensResponse> {
        if !validate_email(&req.email) {
            return Err(PlatformError::invalid_field("email", "invalid email"));
        }

        match zxcvbn::zxcvbn(&req.password, &[&req.email]) {
            Ok(ent) if ent.score() < 3 => {
                return Err(PlatformError::invalid_field(
                    "password",
                    "password is too simple",
                ));
            }
            Err(err) => return Err(PlatformError::invalid_field("password", err.to_string())),
            _ => {}
        }

        let password_hash = make_password_hash(req.password.as_str())
            .map_err(|err| err.wrap_into(AuthError::InvalidPasswordHashing))?;

        let maybe_account = self.accounts.create(CreateAccountRequest {
            created_by: ADMIN_ID,
            password_hash,
            email: req.email,
            name: req.name,
            force_update_password: false,
            role: None,
            organizations: None,
            projects: None,
            teams: None,
        });

        let account = match maybe_account {
            Ok(account) => account,
            Err(MetadataError::AlreadyExists(_)) => {
                return Err(PlatformError::AlreadyExists(
                    "account already exists".to_string(),
                ));
            }
            Err(other) => return Err(other.into()),
        };

        let tokens = self.make_tokens(account.id)?;

        Ok(tokens)
    }

    pub async fn log_in(&self, req: LogInRequest) -> Result<TokensResponse> {
        if !validate_email(&req.email) {
            return Err(PlatformError::invalid_field("email", "invalid email"));
        }

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
        let refresh_claims = parse_refresh_token(refresh_token, "refresh")
            .map_err(|err| err.wrap_into(AuthError::InvalidRefreshToken))?;
        let tokens = self.make_tokens(refresh_claims.account_id)?;

        Ok(tokens)
    }

    pub async fn get(&self, ctx: Context) -> Result<Account> {
        match self.accounts.get_by_id(ctx.account_id) {
            Ok(acc) => Ok(acc.into()),
            Err(MetadataError::NotFound(_)) => {
                Err(PlatformError::NotFound("account not found".to_string()))
            }
            Err(err) => Err(err.into()),
        }
    }

    pub async fn update_name(&self, ctx: Context, req: String) -> Result<()> {
        if req.is_empty() {
            return Err(PlatformError::invalid_field("name", "empty name"));
        }
        let md_req = UpdateAccountRequest {
            updated_by: ctx.account_id,
            name: OptionalProperty::Some(Some(req)),
            email: OptionalProperty::None,
            role: OptionalProperty::None,
            organizations: OptionalProperty::None,
            projects: OptionalProperty::None,
            teams: OptionalProperty::None,
            password_hash: OptionalProperty::None,
            force_update_password: OptionalProperty::None,
        };

        self.accounts.update(ctx.account_id, md_req)?;

        Ok(())
    }

    pub async fn update_email(
        &self,
        ctx: Context,
        req: UpdateEmailRequest,
    ) -> Result<TokensResponse> {
        if !validate_email(&req.email) {
            return Err(PlatformError::invalid_field("email", "invalid email"));
        }

        let account = self.accounts.get_by_id(ctx.account_id)?;

        if let Err(err) = verify_password(
            &req.password,
            PasswordHash::new(account.password_hash.as_str())?,
        ) {
            return Err(PlatformError::invalid_field("password", err.to_string()));
        }

        let md_req = UpdateAccountRequest {
            updated_by: ctx.account_id,
            name: OptionalProperty::None,
            email: OptionalProperty::Some(req.email),
            role: OptionalProperty::None,
            organizations: OptionalProperty::None,
            projects: OptionalProperty::None,
            teams: OptionalProperty::None,
            password_hash: OptionalProperty::None,
            force_update_password: OptionalProperty::None,
        };

        match self.accounts.update(ctx.account_id, md_req) {
            Ok(_) => {}
            Err(MetadataError::AlreadyExists(_)) => {
                return Err(PlatformError::invalid_field(
                    "email",
                    "email already exists",
                ));
            }
            Err(other) => return Err(other.into()),
        };

        let tokens = self.make_tokens(account.id)?;

        Ok(tokens)
    }

    pub async fn update_password(
        &self,
        ctx: Context,
        req: UpdatePasswordRequest,
    ) -> Result<TokensResponse> {
        let account = self.accounts.get_by_id(ctx.account_id)?;

        if verify_password(
            &req.password,
            PasswordHash::new(account.password_hash.as_str())?,
        )
        .is_err()
        {
            return Err(PlatformError::invalid_field("password", "invalid password"));
        }

        let password_hash = make_password_hash(req.new_password.as_str())
            .map_err(|err| err.wrap_into(AuthError::InvalidPasswordHashing))?;

        let md_req = UpdateAccountRequest {
            updated_by: ctx.account_id,
            name: OptionalProperty::None,
            email: OptionalProperty::None,
            role: OptionalProperty::None,
            organizations: OptionalProperty::None,
            projects: OptionalProperty::None,
            teams: OptionalProperty::None,
            password_hash: OptionalProperty::Some(password_hash),
            force_update_password: OptionalProperty::None,
        };

        self.accounts.update(ctx.account_id, md_req)?;

        let tokens = self.make_tokens(account.id)?;

        Ok(tokens)
    }

    pub async fn set_password(
        &self,
        ctx: Context,
        req: SetPasswordRequest,
    ) -> Result<TokensResponse> {
        let account = self.accounts.get_by_id(ctx.account_id)?;
        if !account.force_update_password {
            return Err(PlatformError::Forbidden("forbidden".to_string()));
        }
        let password_hash = make_password_hash(req.password.as_str())
            .map_err(|err| err.wrap_into(AuthError::InvalidPasswordHashing))?;

        let md_req = UpdateAccountRequest {
            updated_by: ctx.account_id,
            name: OptionalProperty::None,
            email: OptionalProperty::None,
            role: OptionalProperty::None,
            organizations: OptionalProperty::None,
            projects: OptionalProperty::None,
            teams: OptionalProperty::None,
            password_hash: OptionalProperty::Some(password_hash),
            force_update_password: OptionalProperty::Some(false),
        };

        self.accounts.update(ctx.account_id, md_req)?;

        let tokens = self.make_tokens(account.id)?;

        Ok(tokens)
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SignUpRequest {
    pub email: String,
    pub password: String,
    pub password_repeat: String,
    pub name: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LogInRequest {
    pub email: String,
    pub password: String,
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
    pub password: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct UpdatePasswordRequest {
    pub password: String,
    pub new_password: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SetPasswordRequest {
    pub password: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Profile {
    pub name: Option<String>,
    pub email: String,
    pub force_update_password: bool,
}
