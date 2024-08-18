use std::net::AddrParseError;
use std::result;

use arrow::error::ArrowError;
use chrono::OutOfRangeError;
use common::error::CommonError;
use datafusion::error::DataFusionError;
use events_gen::error::EventsGenError;
use ingester::error::IngesterError;
use maxminddb::MaxMindDBError;
use metadata::error::MetadataError;
use platform::PlatformError;
use query::error::QueryError;
use storage::error::StoreError;
use thiserror::Error;
use tokio_cron_scheduler::JobSchedulerError;
use tracing::dispatcher::SetGlobalDefaultError;

pub type Result<T> = result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("PlatformError: {0:?}")]
    Platform(#[from] PlatformError),
    #[error("Internal: {0:?}")]
    Internal(String),
    #[error("Bad Request: {0:?}")]
    BadRequest(String),
    #[error("Backup error: {0:?}")]
    BackupError(String),
    #[error("FileNotFound: {0:?}")]
    FileNotFound(String),
    #[error("IP Address Parse Error: {0:?}")]
    AddrParseError(#[from] AddrParseError),
    #[error("StdIO: {0:?}")]
    StdIO(#[from] std::io::Error),
    #[error("QueryError: {0:?}")]
    Query(#[from] QueryError),
    #[error("CommonError: {0:?}")]
    Common(#[from] CommonError),
    #[error("MetadataError: {0:?}")]
    Store(#[from] StoreError),
    #[error("StoreError: {0:?}")]
    Metadata(#[from] MetadataError),
    #[error("EventsGenError: {0:?}")]
    EventsGen(#[from] EventsGenError),
    #[error("IngesterError: {0:?}")]
    Ingester(#[from] IngesterError),
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
    #[error("Crossbeam: {0:?}")]
    CrossbeamError(#[from] crossbeam_channel::RecvError),
    #[error("maxmind: {0:?}")]
    Maxmind(#[from] MaxMindDBError),
    #[error("hyper: {0:?}")]
    Hyper(#[from] hyper::Error),
    #[error("config: {0:?}")]
    Config(#[from] config::ConfigError),
    #[error("global default: {0:?}")]
    SetGlobalDefaultError(#[from] SetGlobalDefaultError),
    #[error("job scheduler: {0:?}")]
    JobScheduler(#[from] JobSchedulerError),
    #[error("rand: {0:?}")]
    RandError(#[from] rand::Error),
    #[error("openssl: {0:?}")]
    OpenSSLStackError(#[from] openssl::error::ErrorStack),
    #[error("object store: {0:?}")]
    ObjectStoreError(#[from] object_store::Error),
    #[error("zip: {0:?}")]
    ZipError(#[from] zip::result::ZipError),
}
