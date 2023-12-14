use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::result;

use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::Response;
use axum::Json;
use common::error::CommonError;
use common::http::ApiError;
use metadata::error::MetadataError;
use query::error::QueryError;
use serde::Serialize;
use serde::Serializer;
use thiserror::Error;
use tracing::debug;

pub type Result<T> = result::Result<T, PlatformError>;

#[derive(Error, Debug)]
pub enum AuthError {
    #[error("invalid credentials")]
    InvalidCredentials,
    #[error("invalid refresh token")]
    InvalidRefreshToken,
    #[error("password hashing error")]
    InvalidPasswordHashing,
    #[error("can't make access token")]
    CantMakeAccessToken,
    #[error("can't make refresh token")]
    CantMakeRefreshToken,
    #[error("can't parse bearer header")]
    CantParseBearerHeader,
    #[error("can't parse access token")]
    CantParseAccessToken,
}

#[derive(Error, Debug)]
pub enum PlatformError {
    #[error("{1:?} error wrapped into {0:?}")]
    Wrapped(Box<PlatformError>, Box<PlatformError>),
    #[error("invalid fields")]
    InvalidFields(BTreeMap<String, String>),
    #[error("bad request: {0:?}")]
    BadRequest(String),
    #[error("unauthorized: {0:?}")]
    Unauthorized(String),
    #[error("forbidden: {0:?}")]
    Forbidden(String),
    #[error("not found: {0:?}")]
    NotFound(String),
    #[error("internal: {0:?}")]
    Internal(String),
    #[error("entity map {0:?}")]
    EntityMap(String),
    #[error("serde: {0:?}")]
    Serde(#[from] serde_json::Error),
    #[error("password hash")]
    PasswordHash(#[from] password_hash::Error),
    #[error("jsonwebtoken: {0:?}")]
    JSONWebToken(#[from] jsonwebtoken::errors::Error),
    #[error("decimal: {0:?}")]
    Decimal(#[from] rust_decimal::Error),
    #[error("metadata: {0:?}")]
    Metadata(#[from] MetadataError),
    #[error("query: {0:?}")]
    Query(#[from] QueryError),
    #[error("common: {0:?}")]
    Common(#[from] CommonError),
    #[error("session: {0:?}")]
    Auth(#[from] AuthError),
    #[error("axum: {0:?}")]
    Axum(#[from] axum::http::Error),
    #[error("hyper: {0:?}")]
    Hyper(#[from] hyper::Error),
    #[error("other: {0:?}")]
    Other(#[from] anyhow::Error),
}

impl PlatformError {
    pub fn wrap_into(self, err: impl Into<PlatformError>) -> PlatformError {
        PlatformError::Wrapped(Box::new(self), Box::new(err.into()))
    }

    pub fn into_api_error(self) -> ApiError {
        match self {
            PlatformError::Serde(err) => ApiError::bad_request(err.to_string()),
            PlatformError::Decimal(err) => ApiError::bad_request(err.to_string()),
            PlatformError::Metadata(err) => match err {
                MetadataError::AlreadyExists(err) => ApiError::conflict(err.to_string()),
                MetadataError::NotFound(err) => ApiError::not_found(err.to_string()),
                MetadataError::Internal(err) => ApiError::internal(err.to_string()),
                MetadataError::BadRequest(err) => ApiError::bad_request(err.to_string()),
                MetadataError::RocksDb(err) => ApiError::internal(err.to_string()),
                MetadataError::FromUtf8(err) => ApiError::internal(err.to_string()),
                MetadataError::Bincode(err) => ApiError::internal(err.to_string()),
                MetadataError::Io(err) => ApiError::internal(err.to_string()),
                MetadataError::Other(err) => ApiError::internal(err.to_string()),
                MetadataError::Store(err) => ApiError::internal(err.to_string()),
            },
            PlatformError::Query(err) => match err {
                QueryError::Internal(err) => ApiError::internal(err),
                QueryError::Plan(err) => ApiError::internal(err),
                QueryError::Execution(err) => ApiError::internal(err),
                QueryError::DataFusion(err) => ApiError::internal(err.to_string()),
                QueryError::Arrow(err) => ApiError::internal(err.to_string()),
                QueryError::Metadata(err) => ApiError::internal(err.to_string()),
                QueryError::Common(err) => ApiError::internal(err.to_string()),
                QueryError::Store(err) => ApiError::internal(err.to_string()),
            },
            PlatformError::BadRequest(msg) => ApiError::bad_request(msg),
            PlatformError::Internal(msg) => ApiError::internal(msg),
            PlatformError::Common(err) => match err {
                CommonError::DataFusionError(err) => ApiError::internal(err.to_string()),
                CommonError::JWTError(err) => ApiError::internal(err.to_string()),
                CommonError::EntityMapping => ApiError::internal(err.to_string()),
                CommonError::BadRequest(err) => ApiError::bad_request(err),
                CommonError::Serde(err) => ApiError::bad_request(err.to_string()),
                CommonError::General(err) => ApiError::internal(err)
            },
            PlatformError::Auth(err) => match err {
                AuthError::InvalidCredentials => ApiError::unauthorized(err),
                AuthError::InvalidRefreshToken => ApiError::unauthorized(err),
                AuthError::InvalidPasswordHashing => ApiError::internal(err),
                AuthError::CantMakeAccessToken => ApiError::internal(err),
                AuthError::CantMakeRefreshToken => ApiError::internal(err),
                AuthError::CantParseBearerHeader => ApiError::unauthorized(err),
                AuthError::CantParseAccessToken => ApiError::unauthorized(err),
            },
            PlatformError::Unauthorized(err) => ApiError::unauthorized(err),
            PlatformError::Forbidden(err) => ApiError::forbidden(err),
            PlatformError::PasswordHash(err) => ApiError::internal(err.to_string()),
            PlatformError::JSONWebToken(err) => ApiError::internal(err.to_string()),
            PlatformError::Axum(err) => ApiError::internal(err.to_string()),
            PlatformError::Other(err) => ApiError::internal(err.to_string()),
            PlatformError::Hyper(err) => ApiError::internal(err.to_string()),
            PlatformError::Wrapped(_, outer) => outer.into_api_error(),
            PlatformError::InvalidFields(fields) => {
                ApiError::new(StatusCode::BAD_REQUEST).with_fields(fields)
            }
            PlatformError::NotFound(err) => ApiError::not_found(err),
            PlatformError::EntityMap(err) => ApiError::internal(err),
        }
    }
}

#[derive(Default)]
pub struct ValidationError {
    fields: BTreeMap<String, String>,
    _message: Option<String>,
}

impl ValidationError {
    pub fn new() -> Self {
        Self {
            fields: BTreeMap::new(),
            _message: None,
        }
    }

    pub fn push(&mut self, field: impl Into<String>, err: impl Into<String>) {
        self.fields.insert(field.into(), err.into());
    }

    pub fn push_invalid(&mut self, field: impl Into<String>) {
        self.fields
            .insert(field.into(), "invalid field value".into());
    }

    pub fn result(self) -> Result<()> {
        if self.fields.is_empty() {
            Ok(())
        } else {
            Err(PlatformError::InvalidFields(self.fields))
        }
    }
}

impl IntoResponse for PlatformError {
    fn into_response(self) -> Response {
        debug!("PlatformError: {:?}", self);
        self.into_api_error().into_response()
    }
}
