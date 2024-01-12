
use std::result;
use std::string::FromUtf8Error;


use thiserror::Error;




pub type Result<T> = result::Result<T, MetadataError>;

#[derive(Error, Debug)]
pub enum MetadataError {
    #[error("already exists ({0:?}")]
    AlreadyExists(String),
    #[error("not found {0:?}")]
    NotFound(String),
    #[error("bad request {0:?}")]
    BadRequest(String),
    #[error("internal: {0:?}")]
    Internal(String),
    #[error("store {0:?}")]
    Store(#[from] store::error::StoreError),
    #[error("rocksdb: {0:?}")]
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
