use datafusion::error::DataFusionError;
use std::{
    fmt::{self, Display, Formatter},
    result,
};

pub type Result<T> = result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Internal(String),
    DataFusionError(DataFusionError),
    Plan(String),
    BincodeError(bincode::Error),
    RocksDbError(rocksdb::Error),
}

impl Display for Error {
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
        write!(formatter, "{}", self)
    }
}

impl From<Vec<u8>> for Error {
    fn from(err: Vec<u8>) -> Self {
        Self::Internal(unsafe { String::from_utf8_unchecked(err) })
    }
}

impl From<rocksdb::Error> for Error {
    fn from(err: rocksdb::Error) -> Self {
        Self::RocksDbError(err)
    }
}

impl From<DataFusionError> for Error {
    fn from(err: DataFusionError) -> Self {
        Self::DataFusionError(err)
    }
}

impl From<bincode::Error> for Error {
    fn from(err: bincode::Error) -> Self {
        Self::BincodeError(err)
    }
}
