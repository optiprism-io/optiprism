use datafusion_common::DataFusionError;
use jsonwebtoken::errors::Error as JWTError;
use std::{
    result,
};
use thiserror::Error;

pub type Result<T> = result::Result<T, CommonError>;

#[derive(Error, Debug)]
pub enum CommonError {
    #[error("DataFusionError: {0:?}")]
    DataFusionError(#[from] DataFusionError),
    #[error("JWTError: {0:?}")]
    JWTError(#[from] JWTError),
}