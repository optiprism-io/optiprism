use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use common::Error as CommonError;
use events_gen::error::Error as EventsGenError;
use metadata::Error as MetadataError;
use platform::Error as PlatformError;
use query::Error as QueryError;

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
    DataFusionError(DataFusionError),
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            Error::Internal(err) => write!(f, "Internal error: {:?}", err),
            Error::PlatformError(err) => write!(f, "PlatformError error: {}", err),
            Error::QueryError(err) => write!(f, "QueryError error: {}", err),
            Error::CommonError(err) => write!(f, "CommonError error: {}", err),
            Error::MetadataError(err) => write!(f, "MetadataError error: {}", err),
            Error::ExternalError(err) => write!(f, "ExternalError error: {}", err),
            Error::EventsGenError(err) => write!(f, "EventsGenError error: {}", err),
            Error::DataFusionError(err) => write!(f, "DataFusionError error: {}", err),
        }
    }
}

impl From<DataFusionError> for Error {
    fn from(err: DataFusionError) -> Self {
        Self::DataFusionError(err)
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
