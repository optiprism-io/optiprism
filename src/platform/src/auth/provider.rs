use std::sync::Arc;

use chrono::Duration;
use common::startup_config::StartupConfig;
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
use common::rbac::OrganizationPermission;
use metadata::MetadataProvider;
use super::password::{check_password_complexity, make_password_hash};
use super::password::verify_password;
use super::token::make_access_token;
use super::token::make_refresh_token;
use super::token::parse_refresh_token;
use crate::accounts::Account;
use crate::error::AuthError;
use crate::Context;
use crate::organizations::Organizations;
use crate::PlatformError;
use crate::Result;
use metadata::organizations::Organizations as MDOrganizations;

#[derive(Clone)]
pub struct Auth {
    md:Arc<MetadataProvider>,
    cfg: StartupConfig,
}

impl Auth {
    pub fn new(md: Arc<MetadataProvider>, cfg: StartupConfig) -> Self {
        Self { md, cfg }
    }

    fn make_tokens(&self, account_id: u64, organisation_id: u64) -> Result<TokensResponse> {
        let sys_cfg=self.md.config.load()?;
        Ok(TokensResponse {
            access_token: make_access_token(
                account_id,
                organisation_id,
                self.cfg.auth.access_token_duration.clone(),
                sys_cfg.auth.access_token.expect("access_token not found"),
            )
                .map_err(|err| err.wrap_into(AuthError::CantMakeAccessToken))?,
            refresh_token: make_refresh_token(
                account_id,
                self.cfg.auth.refresh_token_duration.clone(),
                sys_cfg.auth.refresh_token.expect("refresh_token not found"),
            )
                .map_err(|err| err.wrap_into(AuthError::CantMakeRefreshToken))?,
        })
    }

    pub async fn sign_up(&self, req: SignUpRequest) -> Result<TokensResponse> {
        if !validate_email(&req.email) {
            return Err(PlatformError::invalid_field("email", "invalid email"));
        }

        check_password_complexity(&req.password, &vec![req.email.as_str()])?;

        let password_hash = make_password_hash(req.password.as_str())
            .map_err(|err| err.wrap_into(AuthError::InvalidPasswordHashing))?;

        let maybe_account = self.md.accounts.create(CreateAccountRequest {
            created_by: ADMIN_ID, // todo make it meaningful
            password_hash,
            email: req.email,
            name: req.name,
            force_update_password: false,
            force_update_email: false,
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

        let tokens = self.make_tokens(account.id, 0)?;

        Ok(tokens)
    }

    pub async fn log_in(&self, req: LogInRequest, org_id: Option<u64>) -> Result<TokensResponse> {
        if !validate_email(&req.email) {
            return Err(PlatformError::invalid_field("email", "invalid email"));
        }

        let account = self
            .md.accounts
            .get_by_email(&req.email)
            .map_err(|_err| AuthError::InvalidCredentials)?;

        verify_password(
            req.password,
            PasswordHash::new(account.password_hash.as_str())?,
        )
            .map_err(|_err| AuthError::InvalidCredentials)?;

        // if org_id is provided, check if the account is a member of the org
        let org_id = if let Some(org_id) = org_id {
            let org = self.md.organizations.get_by_id(org_id)?;
            if !org.is_member(account.id) {
                0
            } else {
                org.id
            }
        } else {
            // find first organization
            let orgs = self.md.organizations.list()?;
            let org = orgs.data.iter().find(|org| {
                org.is_member(account.id)
            });
            if let Some(org) = org {
                org.id
            } else {
                0
            }
        };

        let tokens = self.make_tokens(account.id, org_id)?;

        Ok(tokens)
    }

    pub async fn refresh_token(&self, ctx: Context, refresh_token: &str) -> Result<TokensResponse> {
        let refresh_claims = parse_refresh_token(refresh_token, self.md.config.load()?.auth.refresh_token.expect("refresh_token not found"))
            .map_err(|err| err.wrap_into(AuthError::InvalidRefreshToken))?;
        let tokens = self.make_tokens(refresh_claims.account_id, ctx.organization_id)?;

        Ok(tokens)
    }

    pub async fn get(&self, ctx: Context) -> Result<Account> {
        match self.md.accounts.get_by_id(ctx.account_id) {
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
            force_update_email: OptionalProperty::None,
        };

        self.md.accounts.update(ctx.account_id, md_req)?;

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

        let account = self.md.accounts.get_by_id(ctx.account_id)?;

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
            force_update_email: OptionalProperty::None,
        };

        match self.md.accounts.update(ctx.account_id, md_req) {
            Ok(_) => {}
            Err(MetadataError::AlreadyExists(_)) => {
                return Err(PlatformError::invalid_field(
                    "email",
                    "email already exists",
                ));
            }
            Err(other) => return Err(other.into()),
        };

        let tokens = self.make_tokens(account.id, ctx.organization_id)?;

        Ok(tokens)
    }

    pub async fn update_password(
        &self,
        ctx: Context,
        req: UpdatePasswordRequest,
    ) -> Result<TokensResponse> {
        let account = self.md.accounts.get_by_id(ctx.account_id)?;

        if verify_password(
            &req.password,
            PasswordHash::new(account.password_hash.as_str())?,
        )
            .is_err()
        {
            return Err(PlatformError::invalid_field("password", "invalid password"));
        }

        check_password_complexity(&req.new_password, &vec![])?;

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
            force_update_email: OptionalProperty::None,
        };

        self.md.accounts.update(ctx.account_id, md_req)?;

        let tokens = self.make_tokens(account.id, ctx.organization_id)?;

        Ok(tokens)
    }

    pub async fn set_password(
        &self,
        ctx: Context,
        req: SetPasswordRequest,
    ) -> Result<TokensResponse> {
        let account = self.md.accounts.get_by_id(ctx.account_id)?;
        if !account.force_update_password {
            return Err(PlatformError::Forbidden("forbidden".to_string()));
        }

        check_password_complexity(&req.password, &vec![])?;

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
            force_update_email: OptionalProperty::None,
        };

        self.md.accounts.update(ctx.account_id, md_req)?;

        let tokens = self.make_tokens(account.id, ctx.organization_id)?;

        Ok(tokens)
    }

    pub async fn set_email(
        &self,
        ctx: Context,
        req: SetEmailRequest,
    ) -> Result<TokensResponse> {
        let account = self.md.accounts.get_by_id(ctx.account_id)?;
        if !account.force_update_email {
            return Err(PlatformError::Forbidden("forbidden".to_string()));
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
            force_update_email: OptionalProperty::Some(false),
        };

        self.md.accounts.update(ctx.account_id, md_req)?;

        let tokens = self.make_tokens(account.id, ctx.organization_id)?;

        Ok(tokens)
    }

    pub async fn switch_organization(
        &self,
        ctx: Context,
        org_id: u64,
    ) -> Result<TokensResponse> {
        ctx.check_organization_permission(org_id, OrganizationPermission::ViewOrganization)?;
        self.md.organizations.get_by_id(org_id)?;
        let tokens = self.make_tokens(ctx.account_id, org_id)?;
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
pub struct SetEmailRequest {
    pub email: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Profile {
    pub name: Option<String>,
    pub email: String,
    pub force_update_password: bool,
    pub force_update_email: bool,
}
