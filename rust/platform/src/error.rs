use std::collections::HashMap;
use std::fmt::Debug;
use std::{error, result};

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use common::error::CommonError;
use serde::Serialize;
use thiserror::Error;

use metadata::error::{
    AccountError, CustomEventError, DatabaseError, DictionaryError, EventError, MetadataError,
    OrganizationError, ProjectError, PropertyError, StoreError,
};
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
    #[error("internal: {0:?}")]
    Internal(String),
    #[error("serde: {0:?}")]
    Serde(#[from] serde_json::Error),
    #[error("decimal: {0:?}")]
    Decimal(#[from] rust_decimal::Error),
    #[error("metadata: {0:?}")]
    Metadata(#[from] MetadataError),
    #[error("query: {0:?}")]
    Query(#[from] QueryError),
    #[error("common: {0:?}")]
    Common(#[from] CommonError),
    #[error("auth: {0:?}")]
    Auth(#[from] AuthError),
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
    pub fn bad_request(err: Box<dyn error::Error>) -> (StatusCode, Self) {
        ErrorResponse::new(err, StatusCode::BAD_REQUEST)
    }

    pub fn forbidden(err: Box<dyn error::Error>) -> (StatusCode, Self) {
        ErrorResponse::new(err, StatusCode::FORBIDDEN)
    }

    pub fn conflict(err: Box<dyn error::Error>) -> (StatusCode, Self) {
        ErrorResponse::new(err, StatusCode::CONFLICT)
    }

    pub fn not_found(err: Box<dyn error::Error>) -> (StatusCode, Self) {
        ErrorResponse::new(err, StatusCode::NOT_FOUND)
    }

    pub fn internal(err: Box<dyn error::Error>) -> (StatusCode, Self) {
        ErrorResponse::new(err, StatusCode::INTERNAL_SERVER_ERROR)
    }

    pub fn new(err: Box<dyn error::Error>, status: StatusCode) -> (StatusCode, Self) {
        (
            status,
            Self {
                error: InnerError {
                    status: status.as_u16(),
                    code: status.to_string(),
                    message: err.to_string(),
                },
                fields: None,
            },
        )
    }

    pub fn new_inner(status: StatusCode, msg: String) -> (StatusCode, Self) {
        (
            status,
            Self {
                error: InnerError {
                    status: status.as_u16(),
                    code: status.to_string(),
                    message: msg.to_string(),
                },
                fields: None,
            },
        )
    }
}

impl IntoResponse for PlatformError {
    fn into_response(self) -> Response {
        let (a, b) = match self {
            PlatformError::Serde(err) => ErrorResponse::bad_request(Box::new(err)),
            PlatformError::Decimal(err) => ErrorResponse::bad_request(Box::new(err)),
            PlatformError::Metadata(err) => match err {
                MetadataError::Database(err) => match err {
                    DatabaseError::ColumnAlreadyExists(_) => ErrorResponse::conflict(Box::new(err)),
                    DatabaseError::TableNotFound(_) => ErrorResponse::not_found(Box::new(err)),
                    DatabaseError::TableAlreadyExists(_) => ErrorResponse::conflict(Box::new(err)),
                },
                MetadataError::Account(err) => match err {
                    AccountError::AccountNotFound(_) => ErrorResponse::not_found(Box::new(err)),
                    AccountError::AccountAlreadyExist(_) => ErrorResponse::conflict(Box::new(err)),
                },
                MetadataError::Organization(err) => match err {
                    OrganizationError::OrganizationNotFound(_) => {
                        ErrorResponse::not_found(Box::new(err))
                    }
                    OrganizationError::OrganizationAlreadyExist(_) => {
                        ErrorResponse::conflict(Box::new(err))
                    }
                },
                MetadataError::Project(err) => match err {
                    ProjectError::ProjectNotFound(_) => ErrorResponse::not_found(Box::new(err)),
                    ProjectError::ProjectAlreadyExist(_) => ErrorResponse::conflict(Box::new(err)),
                },
                MetadataError::Event(err) => match err {
                    EventError::EventNotFound(_) => ErrorResponse::not_found(Box::new(err)),
                    EventError::EventAlreadyExist(_) => ErrorResponse::conflict(Box::new(err)),
                    EventError::PropertyNotFound(_) => ErrorResponse::not_found(Box::new(err)),
                    EventError::PropertyAlreadyExist(_) => ErrorResponse::conflict(Box::new(err)),
                },
                MetadataError::Property(err) => match err {
                    PropertyError::PropertyNotFound(_) => ErrorResponse::not_found(Box::new(err)),
                    PropertyError::PropertyAlreadyExist(_) => {
                        ErrorResponse::conflict(Box::new(err))
                    }
                },
                MetadataError::Dictionary(err) => match err {
                    DictionaryError::KeyNotFound(_) => ErrorResponse::not_found(Box::new(err)),
                    DictionaryError::ValueNotFound(_) => ErrorResponse::not_found(Box::new(err)),
                },
                MetadataError::Store(err) => match err {
                    StoreError::KeyAlreadyExists(_) => ErrorResponse::conflict(Box::new(err)),
                    StoreError::KeyNotFound(_) => ErrorResponse::not_found(Box::new(err)),
                },
                MetadataError::RocksDb(err) => ErrorResponse::internal(Box::new(err)),
                MetadataError::FromUtf8(err) => ErrorResponse::internal(Box::new(err)),
                MetadataError::Bincode(err) => ErrorResponse::internal(Box::new(err)),
                MetadataError::Io(err) => ErrorResponse::internal(Box::new(err)),
                MetadataError::Other(_) => ErrorResponse::internal(Box::new(err)),
                MetadataError::CustomEvent(err) => match err {
                    CustomEventError::EventNotFound(_) => ErrorResponse::not_found(Box::new(err)),
                    CustomEventError::EventAlreadyExist(_) => {
                        ErrorResponse::conflict(Box::new(err))
                    }
                    CustomEventError::RecursionLevelExceeded(_) => {
                        ErrorResponse::bad_request(Box::new(err))
                    }
                    CustomEventError::DuplicateEvent => ErrorResponse::conflict(Box::new(err)),
                    CustomEventError::EmptyEvents => ErrorResponse::bad_request(Box::new(err)),
                },
            },
            PlatformError::Query(err) => match err {
                QueryError::Internal(_) => ErrorResponse::internal(Box::new(err)),
                QueryError::Plan(_) => ErrorResponse::internal(Box::new(err)),
                QueryError::Execution(_) => ErrorResponse::internal(Box::new(err)),
                QueryError::DataFusion(err) => ErrorResponse::internal(Box::new(err)),
                QueryError::Arrow(err) => ErrorResponse::internal(Box::new(err)),
                QueryError::Metadata(err) => ErrorResponse::internal(Box::new(err)),
            },
            PlatformError::BadRequest(msg) => {
                ErrorResponse::new_inner(StatusCode::BAD_REQUEST, msg)
            }
            PlatformError::Internal(_msg) => ErrorResponse::new_inner(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal server error".to_string(),
            ),
            PlatformError::Common(err) => match err {
                CommonError::DataFusionError(_) => ErrorResponse::internal(Box::new(err)),
                CommonError::JWTError(_) => ErrorResponse::internal(Box::new(err)),
            },
            PlatformError::Auth(err) => match err {
                AuthError::InvalidCredentials => ErrorResponse::forbidden(Box::new(err)),
                AuthError::InvalidToken => ErrorResponse::forbidden(Box::new(err))
            }
        };

        (a, Json(b)).into_response()
    }
}
