use std::result;
// use parquet::errors::ParquetError;
use thiserror::Error;
use arrow2::error::Error as ArrowError;
use parquet2::error::Error as ParquetError;

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