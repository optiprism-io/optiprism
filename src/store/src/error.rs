use std::fmt;
use std::fmt::Display;
use std::fmt::Formatter;
use std::result;

use datafusion::error::DataFusionError;

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
