use arrow::error::ArrowError;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use datafusion::error::DataFusionError;
use metadata::error::Error as MetadataError;
use std::fmt::{Display, Formatter};
use std::{fmt, result};
use store::error::StoreError;
pub type Result<T> = result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Internal(String),
    QueryError(String),
    DataFusionError(DataFusionError),
    ArrowError(ArrowError),
    StoreError(StoreError),
    MetadataError(MetadataError),
}

impl Error {
    /// Wraps this [Error] as an [datafusion::error::DataFusionError::Execution].
    pub fn into_datafusion_execution_error(self) -> DataFusionError {
        DataFusionError::Execution(self.to_string())
    }

    /// Wraps this [Error] as an [datafusion::error::DataFusionError::Plan].
    pub fn into_datafusion_plan_error(self) -> DataFusionError {
        DataFusionError::Plan(self.to_string())
    }
}

impl std::error::Error for Error {}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            Error::Internal(desc) => write!(f, "Internal error: {}", desc),
            Error::QueryError(desc) => write!(f, "Query error: {}", desc),
            Error::DataFusionError(err) => write!(f, "DataFusion error: {}", err),
            Error::ArrowError(err) => write!(f, "ArrowError error: {}", err),
            Error::StoreError(err) => write!(f, "Store error: {}", err),
            Error::MetadataError(err) => write!(f, "Metadata error: {}", err),
        }
    }
}

impl From<DataFusionError> for Error {
    fn from(err: DataFusionError) -> Self {
        Self::DataFusionError(err)
    }
}

impl From<ArrowError> for Error {
    fn from(err: ArrowError) -> Self {
        Self::ArrowError(err)
    }
}

impl From<StoreError> for Error {
    fn from(err: StoreError) -> Self {
        Self::StoreError(err)
    }
}

impl From<MetadataError> for Error {
    fn from(err: MetadataError) -> Self {
        Self::MetadataError(err)
    }
}

impl From<Error> for ArrowError {
    fn from(e: Error) -> Self {
        match e {
            Error::ArrowError(e) => e,
            other => ArrowError::ExternalError(Box::new(other)),
        }
    }
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal server error".to_string(),
        )
            .into_response()
    }
}
