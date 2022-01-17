use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use datafusion::error::DataFusionError;
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
    DataFusionError(DataFusionError),
    Plan(String),
    Internal(InternalError),
    JWTError(jsonwebtoken::errors::Error),
    BincodeError(bincode::Error),
}

impl Display for Error {
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
        write!(formatter, "{}", self)
    }
}

impl From<DataFusionError> for Error {
    fn from(err: DataFusionError) -> Self {
        Self::DataFusionError(err)
    }
}

impl From<jsonwebtoken::errors::Error> for Error {
    fn from(err: jsonwebtoken::errors::Error) -> Self {
        Self::JWTError(err)
    }
}

impl From<InternalError> for Error {
    fn from(err: InternalError) -> Self {
        Self::Internal(err)
    }
}

impl From<bincode::Error> for Error {
    fn from(err: bincode::Error) -> Self {
        Self::BincodeError(err)
    }
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        if let Error::Internal(err) = self {
            (err.status_code, err.code)
        } else {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("{}", self).as_str(),
            )
        }
        .into_response()
    }
}
