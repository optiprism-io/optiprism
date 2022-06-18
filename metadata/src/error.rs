use std::string::FromUtf8Error;
use std::{
    fmt::{self, Display, Formatter},
    result,
};

pub type Result<T> = result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Internal(String),
    Plan(String),
    BincodeError(bincode::Error),
    RocksDbError(rocksdb::Error),
    KeyAlreadyExists,
    KeyNotFound(String),
    ConstraintViolation,
    FromUtf8Error(FromUtf8Error),
}

impl std::error::Error for Error {}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            Error::Internal(err) => write!(f, "Internal error: {}", err),
            Error::Plan(err) => write!(f, "Plan error: {}", err),
            Error::BincodeError(err) => write!(f, "BincodeError error: {}", err),
            Error::RocksDbError(err) => write!(f, "RocksDbError error: {}", err),
            Error::KeyAlreadyExists => write!(f, "KeyAlreadyExists"),
            Error::KeyNotFound(err) => write!(f, "KeyNotFound: {}", err),
            Error::ConstraintViolation => write!(f, "ConstraintViolation"),
            Error::FromUtf8Error(err) => write!(f, "FromUtf8Error: {}", err),
        }

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

impl From<FromUtf8Error> for Error {
    fn from(err: FromUtf8Error) -> Self {
        Self::FromUtf8Error(err)
    }
}
