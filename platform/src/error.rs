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
    Internal(String),
    Internal2(InternalError),
    BadRequest(String),
    SerdeError(serde_json::Error),
    DecimalError(rust_decimal::Error),
    CommonError(CommonError),
    MetadataError(MetadataError),
    QueryError(QueryError),
}

impl std::error::Error for Error {}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            Error::Internal(err) => write!(f, "Internal error: {}", err),
            Error::Internal2(err) => write!(f, "Internal2 error: {:?}", err),
            Error::BadRequest(err) => write!(f, "BadRequest error: {}", err),
            Error::SerdeError(err) => write!(f, "SerdeError error: {}", err),
            Error::DecimalError(err) => write!(f, "DecimalError error: {}", err),
            Error::CommonError(err) => write!(f, "CommonError error: {}", err),
            Error::MetadataError(err) => write!(f, "MetadataError error: {}", err),
            Error::QueryError(err) => write!(f, "QueryError error: {}", err),
        }
    }
}

impl From<InternalError> for Error {
    fn from(err: InternalError) -> Self {
        Self::Internal2(err)
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

impl From<rust_decimal::Error> for Error {
    fn from(err: rust_decimal::Error) -> Self {
        Self::DecimalError(err)
    }
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        match self {
            Error::Internal(err) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("internal error: {:?}", err),
            ),
            Error::Internal2(err) => (err.status_code, err.code.to_string()),

            Error::QueryError(err) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("query error: {:?}", err),
            ),
            _ => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal server error".to_string(),
            ),
        }
        .into_response()
    }
}
