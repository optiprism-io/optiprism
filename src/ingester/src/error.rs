use std::error;
use std::result;

use datafusion_common::DataFusionError;
use jsonwebtoken::errors::Error as JWTError;
use thiserror::Error;

pub type Result<T> = result::Result<T, IngesterError>;
pub type GenericError = Box<dyn error::Error + Send + Sync>;

#[derive(Error, Debug)]
pub enum IngesterError {
    #[error("General: {0:?}")]
    General(String),
}
