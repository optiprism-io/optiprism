use std::result;

use axum::response::IntoResponse;
use axum::response::Response;
use common::http::ApiError;
use maxminddb::MaxMindDBError;
use metadata::error::MetadataError;
use storage::error::StoreError;
use thiserror::Error;

pub type Result<T> = result::Result<T, IngesterError>;

#[derive(Error, Debug)]
pub enum IngesterError {
    #[error("General: {0:?}")]
    Internal(String),
    #[error("Bad request: {0:?}")]
    BadRequest(String),
    #[error("hyper: {0:?}")]
    Hyper(#[from] hyper::Error),
    #[error("metadata: {0:?}")]
    Metadata(#[from] MetadataError),
    #[error("store: {0:?}")]
    Store(#[from] StoreError),
    #[error("maxmind: {0:?}")]
    Maxmind(#[from] MaxMindDBError),
}

impl IntoResponse for IngesterError {
    fn into_response(self) -> Response {
        match self {
            IngesterError::Internal(err) => ApiError::internal(err).into_response(),
            IngesterError::Hyper(err) => ApiError::internal(err).into_response(),
            IngesterError::Metadata(err) => match err {
                MetadataError::AlreadyExists(err) => ApiError::conflict(err).into_response(),
                MetadataError::NotFound(err) => ApiError::not_found(err).into_response(),
                MetadataError::Internal(err) => ApiError::internal(err).into_response(),
                _ => ApiError::internal(err.to_string()).into_response(),
            },
            IngesterError::Maxmind(err) => ApiError::internal(err).into_response(),
            IngesterError::BadRequest(err) => ApiError::bad_request(err).into_response(),
            IngesterError::Store(err) => ApiError::internal(err).into_response(),
        }
    }
}
