use std::fmt::{Display, Formatter};
use std::{fmt, result};
use datafusion::arrow::error::ArrowError;
use datafusion::error::DataFusionError;
use store::error::StoreError;

pub type Result<T> = result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Internal(String),
    DataFusionError(DataFusionError),
    ArrowError(ArrowError),
    StoreError(StoreError),
}

impl Display for Error {
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
        write!(formatter, "{}", self)
    }
}

impl From<DataFusionError> for Error {
    fn from(err: DataFusionError) -> Self {
        Self::DataFusionError(err)
    }
}

impl From<ArrowError> for Error {
    fn from(err: ArrowError) -> Self {
        Self::ArrowError(err)
    }
}

impl From<StoreError> for Error {
    fn from(err: StoreError) -> Self {
        Self::StoreError(err)
    }
}