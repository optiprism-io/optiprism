use std::collections::BTreeMap;
use std::error;
use std::fmt::Display;
use std::fmt::Formatter;
use std::result;

use axum::response::IntoResponse;
use axum::response::Response;
use common::http::ApiError;
use common::http::Json;
use hyper::StatusCode;
use metadata::error::MetadataError;
use serde::Serialize;
use serde::Serializer;
use thiserror::Error;
use tracing::debug;

pub type Result<T> = result::Result<T, IngesterError>;

#[derive(Error, Debug)]
pub enum IngesterError {
    #[error("General: {0:?}")]
    General(String),
    #[error("hyper: {0:?}")]
    Hyper(#[from] hyper::Error),
    #[error("metadata: {0:?}")]
    Metadata(#[from] MetadataError),
}

impl IntoResponse for IngesterError {
    fn into_response(self) -> Response {
        match self {
            IngesterError::General(err) => ApiError {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                code: None,
                message: Some(err),
                fields: Default::default(),
            }
            .into_response(),
            IngesterError::Hyper(err) => ApiError {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                code: None,
                message: Some(err.to_string()),
                fields: Default::default(),
            }
            .into_response(),
            IngesterError::Metadata(err) => ApiError {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                code: None,
                message: Some(err.to_string()),
                fields: Default::default(),
            }
            .into_response(),
        }
    }
}