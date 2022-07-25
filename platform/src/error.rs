use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use std::{
    fmt::{self, Display, Formatter},
    result,
};
use metadata::error::{AccountError, DatabaseError, DictionaryError, EventError, MetadataError, OrganizationError, ProjectError, PropertyError};
use query::error::QueryError;

pub type Result<T> = result::Result<T, PlatformError>;

#[derive(Error, Debug)]
pub enum PlatformError {
    #[error("serde: {0:?}")]
    Serde(#[from] serde_json::Error),
    #[error("decimal: {0:?}")]
    Decimal(#[from] rust_decimal::Error),
    #[error("metadata: {0:?}")]
    Metadata(#[from] MetadataError),
    #[error("query: {0:?}")]
    Query(#[from] QueryError),
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
}

impl ErrorResponse {
    pub fn bad_request(err: impl Into<String>) -> (StatusCode, Self) {
        ErrorResponse::new(err, StatusCode::BAD_REQUEST)
    }

    pub fn conflict(err: impl Into<String>) -> (StatusCode, Self) {
        ErrorResponse::new(err, StatusCode::CONFLICT)
    }

    pub fn not_found(err: impl Into<String>) -> (StatusCode, Self) {
        ErrorResponse::new(err, StatusCode::NOT_FOUND)
    }

    pub fn new(err: impl Into<String>, status: StatusCode) -> (StatusCode, Self) {
        (
            status,
            Self {
                error: InnerError {
                    status: status.as_u16(),
                    code: status.to_string(),
                    message: err.into(),
                }
            }
        )
    }
}

impl IntoResponse for PlatformError {
    fn into_response(self) -> Response {
        match self {
            PlatformError::Serde(err) => ErrorResponse::bad_request(err),
            PlatformError::Decimal(err) => ErrorResponse::bad_request(err),
            PlatformError::Metadata(err) => match err {
                MetadataError::Database(err) => match err {
                    DatabaseError::ColumnAlreadyExists(err) => ErrorResponse::conflict(err),
                    DatabaseError::TableNotFound(err) => ErrorResponse::not_found(err),
                    DatabaseError::TableAlreadyExists(err) => ErrorResponse::conflict(err),
                }
                MetadataError::Account(err) => match err {
                    AccountError::AccountNotFound(err) => ErrorResponse::not_found(err),
                    AccountError::AccountAlreadyExist(err) => ErrorResponse::conflict(err),
                }
                MetadataError::Organization(err) => match err {
                    OrganizationError::OrganizationNotFound(err) => ErrorResponse::not_found(err),
                    OrganizationError::OrganizationAlreadyExist(err) => ErrorResponse::conflict(err),
                }
                MetadataError::Project(err) => match err {
                    ProjectError::ProjectNotFound(_) => ErrorResponse::not_found(err),
                    ProjectError::ProjectAlreadyExist(_) =>  ErrorResponse::conflict(err),
                }
                MetadataError::Event(err) => match err {
                    EventError::EventNotFound(err) => ErrorResponse::not_found(err),
                    EventError::EventAlreadyExist(err) => ErrorResponse::conflict(err),
                    EventError::PropertyNotFound(err) => ErrorResponse::not_found(err),
                    EventError::PropertyAlreadyExist(err) => ErrorResponse::conflict(err),
                }
                MetadataError::Property(err) => match err {
                    PropertyError::PropertyNotFound(err) => ErrorResponse::not_found(err),
                    PropertyError::PropertyAlreadyExist(err) => ErrorResponse::conflict(err),
                }
                MetadataError::Dictionary(err) => match err {
                    DictionaryError::KeyNotFound(_) => {}
                    DictionaryError::ValueNotFound(_) => {}
                }
                MetadataError::Store(_) => {}
                MetadataError::RocksDb(_) => {}
                MetadataError::FromUtf8(_) => {}
                MetadataError::Bincode(_) => {}
                MetadataError::Io(_) => {}
                MetadataError::Other(_) => {}
            }
            PlatformError::Query(err) => match err {
                QueryError::Internal(_) => {}
                QueryError::Plan(_) => {}
                QueryError::Execution(_) => {}
                QueryError::DataFusion(_) => {}
                QueryError::Arrow(_) => {}
                QueryError::Metadata(_) => {}
            }
        }
            .into_response()
    }
}
