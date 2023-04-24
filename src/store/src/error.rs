use std::result;

use arrow2::error::Error as ArrowError;
use parquet2::error::Error as ParquetError;
// use parquet::errors::ParquetError;
use thiserror::Error;

pub type Result<T> = result::Result<T, StoreError>;

#[derive(Error, Debug)]
pub enum StoreError {
    #[error("internal {0:?}")]
    Internal(String),
    #[error("invalid parameter {0:?}")]
    InvalidParameter(String),
    #[error("not yet supported {0:?}")]
    NotYetSupported(String),
    #[error("parquet {0:?}")]
    Parquet(#[from] ParquetError),
    #[error("execution {0:?}")]
    Execution(String),
    #[error("arrow {0:?}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("arrow2 {0:?}")]
    Arrow2(#[from] arrow2::error::Error),
}

impl StoreError {
    pub fn nyi<T>(msg: impl Into<String>) -> Result<T> {
        Err(StoreError::NotYetSupported(msg.into()))
    }
}
