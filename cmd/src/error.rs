use std::{
    result,
};

use datafusion::error::DataFusionError;
use thiserror::Error;

use common::error::CommonError;
use events_gen::error::EventsGenError;
use metadata::error::MetadataError;
use platform::PlatformError;
use query::error::QueryError;

pub type Result<T> = result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("PlatformError: {0:?}")]
    PlatformError(#[from] PlatformError),
    #[error("QueryError: {0:?}")]
    QueryError(#[from] QueryError),
    #[error("CommonError: {0:?}")]
    CommonError(#[from] CommonError),
    #[error("MetadataError: {0:?}")]
    MetadataError(#[from] MetadataError),
    #[error("EventsGenError: {0:?}")]
    EventsGenError(#[from] EventsGenError),
    #[error("DataFusionError: {0:?}")]
    DataFusionError(#[from] DataFusionError),
    #[error("ExternalError: {0:?}")]
    ExternalError(String),
}