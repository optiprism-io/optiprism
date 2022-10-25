use arrow::error::ArrowError;
use datafusion::error::DataFusionError;
use std::result;

pub type Result<T> = result::Result<T, Error>;
#[derive(Debug)]
#[allow(missing_docs)]
pub enum Error {
    ArrowError(ArrowError),
    DataFusionError(DataFusionError),
}
