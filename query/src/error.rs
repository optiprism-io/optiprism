use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use datafusion::arrow::error::ArrowError;
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
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
        write!(formatter, "{}", self)
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

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        match self {
            _ => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal server error".to_string(),
            ),
        }
            .into_response()
    }
}
