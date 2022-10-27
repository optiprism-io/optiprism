use std::collections::HashMap;
use std::fmt::Debug;
use std::result;

use axum::async_trait;
use axum::extract::FromRequest;
use axum::extract::RequestParts;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::Response;
use axum::Json;
use common::error::CommonError;
use hyper::Body;
use hyper::Request;
use metadata::error::AccountError;
use metadata::error::CustomEventError;
use metadata::error::DatabaseError;
use metadata::error::DictionaryError;
use metadata::error::EventError;
use metadata::error::MetadataError;
use metadata::error::OrganizationError;
use metadata::error::ProjectError;
use metadata::error::PropertyError;
use metadata::error::StoreError;
use metadata::error::TeamError;
use query::error::QueryError;
use serde::Serialize;
use thiserror::Error;

use crate::http;

pub type Result<T> = result::Result<T, PlatformError>;

#[derive(Error, Debug)]
pub enum AuthError {
    #[error("invalid credentials")]
    InvalidCredentials,
    #[error("invalid token")]
    InvalidToken,
}

#[derive(Error, Debug)]
pub enum PlatformError {
    #[error("bad request: {0:?}")]
    BadRequest(String),
    #[error("unauthorized: {0:?}")]
    Unauthorized(String),
    #[error("forbidden: {0:?}")]
    Forbidden(String),
    #[error("internal: {0:?}")]
    Internal(String),
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

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
struct InnerError {
    status: u16,
    code: String,
    message: String,
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
struct ErrorResponse {
    error: InnerError,
    fields: Option<HashMap<String, String>>,
}

impl ErrorResponse {
    pub fn bad_request(err: String) -> (StatusCode, Self) {
        ErrorResponse::new(err, StatusCode::BAD_REQUEST)
    }

    pub fn forbidden(err: String) -> (StatusCode, Self) {
        ErrorResponse::new(err, StatusCode::FORBIDDEN)
    }

    pub fn unauthorized(err: String) -> (StatusCode, Self) {
        ErrorResponse::new(err, StatusCode::UNAUTHORIZED)
    }

    pub fn conflict(err: String) -> (StatusCode, Self) {
        ErrorResponse::new(err, StatusCode::CONFLICT)
    }

    pub fn not_found(err: String) -> (StatusCode, Self) {
        ErrorResponse::new(err, StatusCode::NOT_FOUND)
    }

    pub fn internal(err: String) -> (StatusCode, Self) {
        ErrorResponse::new(err, StatusCode::INTERNAL_SERVER_ERROR)
    }

    pub fn new(err: String, status: StatusCode) -> (StatusCode, Self) {
        (status, Self {
            error: InnerError {
                status: status.as_u16(),
                code: status.to_string(),
                message: err,
            },
            fields: None,
        })
    }
}

impl IntoResponse for PlatformError {
    fn into_response(self) -> Response {
        let (status_code, error_response) = match self {
            PlatformError::Serde(err) => ErrorResponse::bad_request(err.to_string()),
            PlatformError::Decimal(err) => ErrorResponse::bad_request(err.to_string()),
            PlatformError::Metadata(err) => match err {
                MetadataError::Database(err) => match err {
                    DatabaseError::ColumnAlreadyExists(_) => {
                        ErrorResponse::conflict(err.to_string())
                    }
                    DatabaseError::TableNotFound(_) => ErrorResponse::not_found(err.to_string()),
                    DatabaseError::TableAlreadyExists(_) => {
                        ErrorResponse::conflict(err.to_string())
                    }
                },
                MetadataError::Account(err) => match err {
                    AccountError::AccountNotFound(_) => ErrorResponse::not_found(err.to_string()),
                    AccountError::AccountAlreadyExist(_) => {
                        ErrorResponse::conflict(err.to_string())
                    }
                },
                MetadataError::Organization(err) => match err {
                    OrganizationError::OrganizationNotFound(_) => {
                        ErrorResponse::not_found(err.to_string())
                    }
                    OrganizationError::OrganizationAlreadyExist(_) => {
                        ErrorResponse::conflict(err.to_string())
                    }
                },
                MetadataError::Project(err) => match err {
                    ProjectError::ProjectNotFound(_) => ErrorResponse::not_found(err.to_string()),
                    ProjectError::ProjectAlreadyExist(_) => {
                        ErrorResponse::conflict(err.to_string())
                    }
                },
                MetadataError::Event(err) => match err {
                    EventError::EventNotFound(_) => ErrorResponse::not_found(err.to_string()),
                    EventError::EventAlreadyExist(_) => ErrorResponse::conflict(err.to_string()),
                    EventError::PropertyNotFound(_) => ErrorResponse::not_found(err.to_string()),
                    EventError::PropertyAlreadyExist(_) => ErrorResponse::conflict(err.to_string()),
                },
                MetadataError::Property(err) => match err {
                    PropertyError::PropertyNotFound(_) => ErrorResponse::not_found(err.to_string()),
                    PropertyError::PropertyAlreadyExist(_) => {
                        ErrorResponse::conflict(err.to_string())
                    }
                },
                MetadataError::Dictionary(err) => match err {
                    DictionaryError::KeyNotFound(_) => ErrorResponse::not_found(err.to_string()),
                    DictionaryError::ValueNotFound(_) => ErrorResponse::not_found(err.to_string()),
                },
                MetadataError::Store(err) => match err {
                    StoreError::KeyAlreadyExists(_) => ErrorResponse::conflict(err.to_string()),
                    StoreError::KeyNotFound(_) => ErrorResponse::not_found(err.to_string()),
                },
                MetadataError::RocksDb(err) => ErrorResponse::internal(err.to_string()),
                MetadataError::FromUtf8(err) => ErrorResponse::internal(err.to_string()),
                MetadataError::Bincode(err) => ErrorResponse::internal(err.to_string()),
                MetadataError::Io(err) => ErrorResponse::internal(err.to_string()),
                MetadataError::Other(_) => ErrorResponse::internal(err.to_string()),
                MetadataError::CustomEvent(err) => match err {
                    CustomEventError::EventNotFound(_) => ErrorResponse::not_found(err.to_string()),
                    CustomEventError::EventAlreadyExist(_) => {
                        ErrorResponse::conflict(err.to_string())
                    }
                    CustomEventError::RecursionLevelExceeded(_) => {
                        ErrorResponse::bad_request(err.to_string())
                    }
                    CustomEventError::DuplicateEvent => ErrorResponse::conflict(err.to_string()),
                    CustomEventError::EmptyEvents => ErrorResponse::bad_request(err.to_string()),
                },
                MetadataError::Team(err) => match err {
                    TeamError::TeamNotFound(_) => ErrorResponse::not_found(err.to_string()),
                    TeamError::TeamAlreadyExist(_) => ErrorResponse::conflict(err.to_string()),
                },
            },
            PlatformError::Query(err) => match err {
                QueryError::Internal(err) => ErrorResponse::internal(err),
                QueryError::Plan(err) => ErrorResponse::internal(err),
                QueryError::Execution(err) => ErrorResponse::internal(err),
                QueryError::DataFusion(err) => ErrorResponse::internal(err.to_string()),
                QueryError::Arrow(err) => ErrorResponse::internal(err.to_string()),
                QueryError::Metadata(err) => ErrorResponse::internal(err.to_string()),
            },
            PlatformError::BadRequest(msg) => ErrorResponse::bad_request(msg),
            PlatformError::Internal(msg) => ErrorResponse::internal(msg),
            PlatformError::Common(err) => match err {
                CommonError::DataFusionError(err) => ErrorResponse::internal(err.to_string()),
                CommonError::JWTError(err) => ErrorResponse::internal(err.to_string()),
                CommonError::EntityMapping => ErrorResponse::internal(err.to_string()),
            },
            PlatformError::Auth(err) => match err {
                AuthError::InvalidCredentials => ErrorResponse::forbidden(err.to_string()),
                AuthError::InvalidToken => ErrorResponse::forbidden(err.to_string()),
            },
            PlatformError::Unauthorized(err) => ErrorResponse::unauthorized(err),
            PlatformError::Forbidden(err) => ErrorResponse::forbidden(err),
            PlatformError::PasswordHash(err) => ErrorResponse::internal(err.to_string()),
            PlatformError::JSONWebToken(err) => ErrorResponse::internal(err.to_string()),
            PlatformError::Axum(err) => ErrorResponse::internal(err.to_string()),
            PlatformError::Other(err) => ErrorResponse::internal(err.to_string()),
            PlatformError::Hyper(err) => ErrorResponse::internal(err.to_string()),
        };

        (status_code, Json(error_response)).into_response()
    }
}
