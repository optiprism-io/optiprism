use arrow::error::ArrowError;
use std::net::AddrParseError;
use std::result;
use time::OutOfRangeError;

use datafusion::error::DataFusionError;
use thiserror::Error;

use common::error::CommonError;
use events_gen::error::EventsGenError;
use metadata::error::MetadataError;
use platform::PlatformError;
use query::error::QueryError;

pub type Result<T> = result::Result<T, DemoError>;

#[derive(Error, Debug)]
pub enum DemoError {
    #[error("PlatformError: {0:?}")]
    Platform(#[from] PlatformError),
    #[error("Internal")]
    Internal(String),
    #[error("IP Address Parse Error: {0:?}")]
    AddrParseError(#[from] AddrParseError),
    #[error("StdIO: {0:?}")]
    StdIO(#[from] std::io::Error),
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
    #[error("ArrowError: {0:?}")]
    ArrowError(#[from] ArrowError),
    #[error("CSVError: {0:?}")]
    CSVError(#[from] csv::Error),
    #[error("ParseDuration: {0:?}")]
    ParseDuration(#[from] parse_duration::parse::Error),
    #[error("other: {0:?}")]
    Other(#[from] anyhow::Error),
    #[error("TimeDurationOutOfRange: {0:?}")]
    TimeDurationOutOfRange(#[from] OutOfRangeError),
}
