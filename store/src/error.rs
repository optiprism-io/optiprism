use datafusion::error::DataFusionError;
use std::fmt::{Display, Formatter};
use std::{fmt, result};

pub type Result<T> = result::Result<T, StoreError>;

#[derive(Debug)]
pub enum StoreError {
    DataFusionError(DataFusionError),
    Plan(String),
}

impl Display for StoreError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            StoreError::Plan(desc) => write!(f, "Plan error: {}", desc),
            StoreError::DataFusionError(err) => write!(f, "DataFusion error: {}", err),
        }
    }
}

impl From<DataFusionError> for StoreError {
    fn from(err: DataFusionError) -> Self {
        Self::DataFusionError(err)
    }
}
