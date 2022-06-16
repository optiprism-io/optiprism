use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use platform::Error as PlatformError;
use query::Error as QueryError;
use common::Error as CommonError;
use metadata::Error as MetadataError;
use events_gen::error::Error as EventsGenError;

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
    PlatformError(PlatformError),
    QueryError(QueryError),
    CommonError(CommonError),
    MetadataError(MetadataError),
    ExternalError(String),
    EventsGenError(EventsGenError),
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

impl From<PlatformError> for Error {
    fn from(err: PlatformError) -> Self {
        Self::PlatformError(err)
    }
}

impl From<QueryError> for Error {
    fn from(err: QueryError) -> Self {
        Self::QueryError(err)
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

impl From<EventsGenError> for Error {
    fn from(err: EventsGenError) -> Self {
        Self::EventsGenError(err)
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