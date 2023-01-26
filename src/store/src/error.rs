use std::result;
use arrow::error::ArrowError;
use parquet::errors::ParquetError;
use thiserror::Error;

pub type Result<T> = result::Result<T, QueryError>;

#[derive(Error, Debug)]
pub enum StoreError {
    #[error("internal {0:?}")]
    Internal(String),
    #[error("parquet {0:?}")]
    Parquet(#[from] ParquetError),
    #[error("execution {0:?}")]
    #[error("arrow {0:?}")]
    Arrow(#[from] ArrowError),
}