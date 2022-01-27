use std::{
    fmt::{self, Display, Formatter},
    result,
};
use std::sync::PoisonError;

pub type Result<T> = result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Internal(String),
    Plan(String),
    BincodeError(bincode::Error),
    RocksDbError(rocksdb::Error),
    KeyAlreadyExist,
    KeyNotFound,
    ConstraintViolation,
}

impl std::error::Error for Error {}

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

impl From<bincode::Error> for Error {
    fn from(err: bincode::Error) -> Self {
        Self::BincodeError(err)
    }
}
