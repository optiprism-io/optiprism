use datafusion::arrow::error::ArrowError;
use datafusion::error::DataFusionError;
use std::fmt::{Display, Formatter};
use std::{fmt, result};
use store::error::StoreError;

pub type Result<T> = result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Internal(String),
    DataFusionError(DataFusionError),
    ArrowError(ArrowError),
    StoreError(StoreError),
}

impl Error {
    /// Wraps this [Error] as an [datafusion::error::DataFusionError::Execution].
    pub fn into_datafusion_execution_error(self) -> DataFusionError {
        DataFusionError::Execution(self.to_string())
    }

    /// Wraps this [Error] as an [datafusion::error::DataFusionError::Plan].
    pub fn into_datafusion_plan_error(self) -> DataFusionError {
        DataFusionError::Plan(self.to_string())
    }

    pub fn into_arrow_external_error(self) -> ArrowError {
        ArrowError::from_external_error(Box::new(self))
    }
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
