use std::fmt::{Display, Formatter};
use std::{fmt, result};
use arrow::error::ArrowError;
use metadata::error::Error as MetadataError;

pub type Result<T> = result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Internal(String),
    External(String),
    ArrowError(ArrowError),
    CSVError(csv::Error),
    MetadataError(MetadataError),
    UserSessionEnded,
}

impl std::error::Error for Error {}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            Error::Internal(desc) => write!(f, "Internal error: {}", desc),
            Error::External(desc) => write!(f, "External error: {}", desc),
            Error::CSVError(err) => write!(f, "CSV error: {}", err),
            Error::UserSessionEnded => write!(f, "User session ended"),
            Error::ArrowError(err) => write!(f, "Arrow error: {}", err)
        }
    }
}

impl From<ArrowError> for Error {
    fn from(err: ArrowError) -> Self {
        Self::ArrowError(err)
    }
}
impl From<csv::Error> for Error {
    fn from(err: csv::Error) -> Self {
        Self::CSVError(err)
    }
}

impl From<MetadataError> for Error {
    fn from(err: MetadataError) -> Self {
        Self::MetadataError(err)
    }
}