use datafusion_common::DataFusionError;
use jsonwebtoken::errors::Error as JWTError;
use std::{error, result};
use thiserror::Error;

pub type Result<T> = result::Result<T, CommonError>;
pub type GenericError = Box<dyn error::Error + Send + Sync>;

#[derive(Error, Debug)]
pub enum CommonError {
    #[error("DataFusionError: {0:?}")]
    DataFusionError(#[from] DataFusionError),
    #[error("JWTError: {0:?}")]
    JWTError(#[from] JWTError),
    #[error("EntityMapping")]
    EntityMapping
}
