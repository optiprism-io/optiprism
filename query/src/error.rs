use arrow::error::ArrowError;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use datafusion::error::DataFusionError;
use metadata::error::MetadataError;
use std::fmt::{Display, Formatter};
use std::{fmt, result};
use store::error::StoreError;
use thiserror::Error;

pub type Result<T> = result::Result<T, QueryError>;

#[derive(Error, Debug)]
pub enum QueryError {
    #[error("internal {0:?}")]
    Internal(String),
    #[error("plan {0:?}")]
    Plan(String),
    #[error("execution {0:?}")]
    Execution(String),
    #[error("datafusion {0:?}")]
    DataFusion(#[from] DataFusionError),
    #[error("arrow {0:?}")]
    Arrow(#[from] ArrowError),
    #[error("metadata {0:?}")]
    Metadata(#[from] MetadataError),
}

impl QueryError {
    /// Wraps this [Error] as an [datafusion::error::DataFusionError::Execution].
    pub fn into_datafusion_execution_error(self) -> DataFusionError {
        DataFusionError::Execution(self.to_string())
    }

    /// Wraps this [Error] as an [datafusion::error::DataFusionError::Plan].
    pub fn into_datafusion_plan_error(self) -> DataFusionError {
        DataFusionError::Plan(self.to_string())
    }
}

impl From<QueryError> for ArrowError {
    fn from(e: QueryError) -> Self {
        ArrowError::ExternalError(Box::new(e))
    }
}