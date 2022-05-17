use std::fmt::{Display, Formatter};
use std::{fmt, result};

pub type Result<T> = result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Internal(String),
    External(String),
    CSVError(csv::Error),
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
        }
    }
}

impl From<csv::Error> for Error {
    fn from(err: csv::Error) -> Self {
        Self::CSVError(err)
    }
}