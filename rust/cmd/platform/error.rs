use std::result;

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
    Platform(#[from] PlatformError),
    #[error("QueryError: {0:?}")]
    Query(#[from] QueryError),
    #[error("CommonError: {0:?}")]
    Common(#[from] CommonError),
    #[error("MetadataError: {0:?}")]
    Metadata(#[from] MetadataError),
    #[error("EventsGenError: {0:?}")]
    EventsGen(#[from] EventsGenError),
    #[error("DataFusionError: {0:?}")]
    DataFusion(#[from] DataFusionError),
}
