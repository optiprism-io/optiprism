use std::fmt::{Display, Formatter};
use std::{fmt, result};
use datafusion::error::DataFusionError;

pub type Result<T> = result::Result<T, StoreError>;

#[derive(Debug)]
pub enum StoreError {
    DataFusionError(DataFusionError),
    Plan(String),
}

impl Display for StoreError {
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
        write!(formatter, "{}", self)
    }
}

impl From<DataFusionError> for StoreError {
    fn from(err: DataFusionError) -> Self {
        Self::DataFusionError(err)
    }
}
