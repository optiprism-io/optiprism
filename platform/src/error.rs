use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use jsonwebtoken::errors::Error as JWTError;
use metadata::error::Error as MetadataError;
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
    const fn new(code: &'static str, status_code: StatusCode) -> Self {
        Self { code, status_code }
    }
}

#[derive(Debug)]
pub enum Error {
    Internal(InternalError),
    MetadataError(MetadataError),
    JWTError(JWTError),
}

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

impl From<MetadataError> for Error {
    fn from(err: MetadataError) -> Self {
        Self::MetadataError(err)
    }
}

impl From<JWTError> for Error {
    fn from(err: JWTError) -> Self {
        Self::JWTError(err)
    }
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        if let Error::Internal(err) = self {
            (err.status_code, err.code.to_string())
        } else {
            (StatusCode::INTERNAL_SERVER_ERROR, format!("{}", self))
        }
        .into_response()
    }
}
