use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use common::Error as CommonError;
use metadata::Error as MetadataError;
use query::Error as QueryError;

use std::{
    fmt::{self, Display, Formatter},
    result,
};

pub type Result<T> = result::Result<T, Error>;

#[derive(Debug, Clone)]
pub struct InternalError {
    code: &'static str,
    status_code: StatusCode,
}

impl InternalError {
    pub fn new(code: &'static str, status_code: StatusCode) -> Self {
        Self { code, status_code }
    }
}

#[derive(Debug)]
pub enum Error {
    Internal(InternalError),
    BadRequest(String),
    SerdeError(serde_json::Error),
    CommonError(CommonError),
    MetadataError(MetadataError),
    QueryError(QueryError),
}

impl std::error::Error for Error {}

impl Display for Error {
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
        write!(formatter, "{}", self)
    }
}

impl From<InternalError> for Error {
    fn from(err: InternalError) -> Self {
        Self::Internal(err)
    }
}

impl From<CommonError> for Error {
    fn from(err: CommonError) -> Self {
        Self::CommonError(err)
    }
}

impl From<MetadataError> for Error {
    fn from(err: MetadataError) -> Self {
        Self::MetadataError(err)
    }
}

impl From<QueryError> for Error {
    fn from(err: QueryError) -> Self {
        Self::QueryError(err)
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Self::SerdeError(err)
    }
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        match self {
            Error::Internal(err) => (err.status_code, err.code.to_string()),
            Error::MetadataError(err) => match err {
                metadata::Error::KeyNotFound(_) => (StatusCode::NOT_FOUND, "not found".to_string()),
                _ => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal server error".to_string(),
                ),
            },
            _ => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal server error".to_string(),
            ),
        }
            .into_response()
    }
}
