use std::error;
use std::result;
use std::string::FromUtf8Error;
use std::sync::PoisonError;

use thiserror::Error;
use tokio::sync::RwLockWriteGuard;

use crate::database::Column;
use crate::database::TableRef;
use crate::properties;

pub type Result<T> = result::Result<T, MetadataError>;

#[derive(Error, Debug)]
pub enum MetadataError {
    #[error("already exists ({0:?}")]
    AlreadyExists(String),
    #[error("not found {0:?}")]
    NotFound(String),
    #[error("internal: {0:?}")]
    Internal(String),
    RocksDb(#[from] rocksdb::Error),
    #[error("from utf {0:?}")]
    FromUtf8(#[from] FromUtf8Error),
    #[error("bincode {0:?}")]
    Bincode(#[from] bincode::Error),
    #[error("io {0}")]
    Io(#[from] std::io::Error),
    #[error("{0:?}")]
    Other(#[from] anyhow::Error),
}
