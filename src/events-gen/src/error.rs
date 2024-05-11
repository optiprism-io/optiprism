use std::result;

use arrow::error::ArrowError;
use ingester::error::IngesterError;
use metadata::error::MetadataError;
use thiserror::Error;

pub type Result<T> = result::Result<T, EventsGenError>;

#[derive(Error, Debug)]
pub enum EventsGenError {
    #[error("Internal: {0:?}")]
    Internal(String),
    #[error("External {0:?}")]
    External(String),
    #[error("ArrowError: {0:?}")]
    ArrowError(#[from] ArrowError),
    #[error("CSVError: {0:?}")]
    CSVError(#[from] csv::Error),
    #[error("MetadataError: {0:?}")]
    MetadataError(#[from] MetadataError),
    #[error("IngesterError: {0:?}")]
    IngesterError(#[from] IngesterError),
    #[error("UserSessionEnded")]
    UserSessionEnded,
    #[error("General {0:?}")]
    General(String),
    #[error("FileNotFound: {0:?}")]
    FileNotFound(String),
    #[error("Other: {0:?}")]
    AnyhowError(#[from] anyhow::Error),
}
