use super::{LogInRequest, SignUpRequest};
use crate::{Context, PlatformError, Result};
use chrono::{Duration, Utc};
use common::{
    rbac::{Permission, Role},
};

use metadata::{
    accounts::CreateAccountRequest as CreateAccountRequest,
    organizations::CreateOrganizationRequest as CreateOrganizationRequest, Metadata,
};
use std::{collections::HashMap, ops::Add, sync::Arc};
use argon2::Algorithm::Argon2d;
use password_hash::PasswordHash;
use metadata::accounts::Account;
use metadata::error::{AccountError, MetadataError};
use crate::auth::auth::{AccessClaims, make_access_token, make_password_hash, verify_password};
use crate::auth::types::{ TokenResponse};
use crate::error::AuthError;

pub struct Provider {
    md: Arc<Metadata>,
    // accesable from http auth mw
    pub access_token_duration: Duration,
    pub access_token_key: String,
}

impl Provider {
    pub fn new(md: Arc<Metadata>, access_token_duration: Duration, access_token_key: String) -> Self {
        Self { md, access_token_duration, access_token_key }
    }

    pub async fn sign_up(&self, ctx: Context, req: SignUpRequest) -> Result<TokenResponse> {
        let account = self
            .md
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


        let access_token = make_access_token(
            self.access_token_duration.clone(),
            account.id,
            &self.access_token_key,
        )?;

        Ok(TokenResponse { access_token })
    }

    pub async fn log_in(&self, _ctx: Context, req: LogInRequest) -> Result<TokensResponse> {
        let account = match self.md.accounts.get_by_email(&req.email).await {
            Ok(account) => account,
            Err(err) => return Err(AuthError::InvalidCredentials.into())
        };

        Ok(verify_password(req.password, PasswordHash::new(account.password_hash.as_str())?)?);

        let access_token = make_access_token(
            self.access_token_duration.clone(),
            account.id,
            &self.access_token_key,
        )?;

        Ok(TokenResponse { access_token })
    }
}


