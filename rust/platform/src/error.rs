use std::collections::HashMap;
use std::fmt::Debug;
use std::{result};

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use common::error::CommonError;
use serde::Serialize;
use thiserror::Error;

use metadata::error::{AccountError, CustomEventError, DatabaseError, DictionaryError, EventError, MetadataError, OrganizationError, ProjectError, PropertyError, StoreError, TeamError};
use query::error::QueryError;

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
    #[error("other: {0:?}")]
    Other(#[from] anyhow::Error)
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
        (
            status,
            Self {
                error: InnerError {
                    status: status.as_u16(),
                    code: status.to_string(),
                    message: err,
                },
                fields: None,
            },
        )
    }

}

impl IntoResponse for PlatformError {
    fn into_response(self) -> Response {
        let (status_code, error_response) = match self {
            PlatformError::Serde(err) => ErrorResponse::bad_request(format!("{:?}", err)),
            PlatformError::Decimal(err) => ErrorResponse::bad_request(format!("{:?}", err)),
            PlatformError::Metadata(err) => match err {
                MetadataError::Database(err) => match err {
                    DatabaseError::ColumnAlreadyExists(_) => ErrorResponse::conflict(format!("{:?}", err)),
                    DatabaseError::TableNotFound(_) => ErrorResponse::not_found(format!("{:?}", err)),
                    DatabaseError::TableAlreadyExists(_) => ErrorResponse::conflict(format!("{:?}", err)),
                },
                MetadataError::Account(err) => match err {
                    AccountError::AccountNotFound(_) => ErrorResponse::not_found(format!("{:?}", err)),
                    AccountError::AccountAlreadyExist(_) => ErrorResponse::conflict(format!("{:?}", err)),
                },
                MetadataError::Organization(err) => match err {
                    OrganizationError::OrganizationNotFound(_) => {
                        ErrorResponse::not_found(format!("{:?}", err))
                    }
                    OrganizationError::OrganizationAlreadyExist(_) => {
                        ErrorResponse::conflict(format!("{:?}", err))
                    }
                },
                MetadataError::Project(err) => match err {
                    ProjectError::ProjectNotFound(_) => ErrorResponse::not_found(format!("{:?}", err)),
                    ProjectError::ProjectAlreadyExist(_) => ErrorResponse::conflict(format!("{:?}", err)),
                },
                MetadataError::Event(err) => match err {
                    EventError::EventNotFound(_) => ErrorResponse::not_found(format!("{:?}", err)),
                    EventError::EventAlreadyExist(_) => ErrorResponse::conflict(format!("{:?}", err)),
                    EventError::PropertyNotFound(_) => ErrorResponse::not_found(format!("{:?}", err)),
                    EventError::PropertyAlreadyExist(_) => ErrorResponse::conflict(format!("{:?}", err)),
                },
                MetadataError::Property(err) => match err {
                    PropertyError::PropertyNotFound(_) => ErrorResponse::not_found(format!("{:?}", err)),
                    PropertyError::PropertyAlreadyExist(_) => {
                        ErrorResponse::conflict(format!("{:?}", err))
                    }
                },
                MetadataError::Dictionary(err) => match err {
                    DictionaryError::KeyNotFound(_) => ErrorResponse::not_found(format!("{:?}", err)),
                    DictionaryError::ValueNotFound(_) => ErrorResponse::not_found(format!("{:?}", err)),
                },
                MetadataError::Store(err) => match err {
                    StoreError::KeyAlreadyExists(_) => ErrorResponse::conflict(format!("{:?}", err)),
                    StoreError::KeyNotFound(_) => ErrorResponse::not_found(format!("{:?}", err)),
                },
                MetadataError::RocksDb(err) => ErrorResponse::internal(format!("{:?}", err)),
                MetadataError::FromUtf8(err) => ErrorResponse::internal(format!("{:?}", err)),
                MetadataError::Bincode(err) => ErrorResponse::internal(format!("{:?}", err)),
                MetadataError::Io(err) => ErrorResponse::internal(format!("{:?}", err)),
                MetadataError::Other(_) => ErrorResponse::internal(format!("{:?}", err)),
                MetadataError::CustomEvent(err) => match err {
                    CustomEventError::EventNotFound(_) => ErrorResponse::not_found(format!("{:?}", err)),
                    CustomEventError::EventAlreadyExist(_) => {
                        ErrorResponse::conflict(format!("{:?}", err))
                    }
                    CustomEventError::RecursionLevelExceeded(_) => {
                        ErrorResponse::bad_request(format!("{:?}", err))
                    }
                    CustomEventError::DuplicateEvent => ErrorResponse::conflict(format!("{:?}", err)),
                    CustomEventError::EmptyEvents => ErrorResponse::bad_request(format!("{:?}", err)),
                },
                MetadataError::Team(err) => match err {
                    TeamError::TeamNotFound(_) => ErrorResponse::not_found(format!("{:?}", err)),
                    TeamError::TeamAlreadyExist(_) => ErrorResponse::conflict(format!("{:?}", err)),
                }
            },
            PlatformError::Query(err) => match err {
                QueryError::Internal(err) => ErrorResponse::internal(err),
                QueryError::Plan(err) => ErrorResponse::internal(err),
                QueryError::Execution(err) => ErrorResponse::internal(err),
                QueryError::DataFusion(err) => ErrorResponse::internal(format!("{:?}", err)),
                QueryError::Arrow(err) => ErrorResponse::internal(format!("{:?}", err)),
                QueryError::Metadata(err) => ErrorResponse::internal(format!("{:?}", err)),
            },
            PlatformError::BadRequest(msg) => ErrorResponse::bad_request(msg),
            PlatformError::Internal(msg) => ErrorResponse::internal(msg),
            PlatformError::Common(err) => match err {
                CommonError::DataFusionError(err) => ErrorResponse::internal(format!("{:?}", err)),
                CommonError::JWTError(err) => ErrorResponse::internal(format!("{:?}", err)),
            },
            PlatformError::Auth(err) => match err {
                AuthError::InvalidCredentials => ErrorResponse::forbidden(format!("{:?}", err)),
                AuthError::InvalidToken => ErrorResponse::forbidden(format!("{:?}", err))
            },
            PlatformError::Unauthorized(err) => ErrorResponse::unauthorized(err),
            PlatformError::Forbidden(err) => ErrorResponse::forbidden(err),
            PlatformError::PasswordHash(err) => ErrorResponse::internal(format!("{:?}", err)),
            PlatformError::JSONWebToken(err) => ErrorResponse::internal(format!("{:?}", err)),
            PlatformError::Axum(err) => ErrorResponse::internal(err.to_string()),
            PlatformError::Other(err) => ErrorResponse::internal(err.to_string()),
        };

        (status_code, Json(error_response)).into_response()
    }
}
