use super::{LogInRequest, SignUpRequest};
use crate::{Context, PlatformError, Result};
use chrono::{Duration, Utc};
use common::rbac::{Permission, Role};

use crate::auth::auth::{make_access_token, make_password_hash, verify_password, AccessClaims};
use crate::auth::types::TokenResponse;
use crate::error::AuthError;
use argon2::Algorithm::Argon2d;
use metadata::accounts::Account;
use metadata::error::{AccountError, MetadataError};
use metadata::{
    accounts::CreateAccountRequest, organizations::CreateOrganizationRequest, Metadata,
};
use password_hash::PasswordHash;
use std::{collections::HashMap, ops::Add, sync::Arc};

#[derive(Clone)]
pub struct Provider {
    accounts: Arc<metadata::accounts::Provider>,
    // accesable from http auth mw
    pub access_token_duration: Duration,
    pub access_token_key: String,
}

impl Provider {
    pub fn new(
        accounts: Arc<metadata::accounts::Provider>,
        access_token_duration: Duration,
        access_token_key: String,
    ) -> Self {
        Self {
            accounts,
            access_token_duration,
            access_token_key,
        }
    }

    pub async fn sign_up(&self, req: SignUpRequest) -> Result<TokenResponse> {
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

        let access_token = make_access_token(
            self.access_token_duration.clone(),
            account.id,
            &self.access_token_key,
        )?;

        Ok(TokenResponse { access_token })
    }

    pub async fn log_in(&self, req: LogInRequest) -> Result<TokenResponse> {
        let account = self
            .accounts
            .get_by_email(&req.email)
            .await
            .map_err(|err| PlatformError::Unauthorized(format!("{:?}", err)))?;

        verify_password(
            req.password.as_str(),
            PasswordHash::new(account.password_hash.as_str())?,
        ).map_err(|err| PlatformError::Unauthorized(format!("{:?}", err)))?;

        let access_token = make_access_token(
            self.access_token_duration.clone(),
            account.id,
            &self.access_token_key,
        ).map_err(|err| PlatformError::Unauthorized(format!("{:?}", err)))?;

        Ok(TokenResponse {
            access_token,
        })
    }
}
