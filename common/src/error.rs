use jsonwebtoken::errors::Error as JWTError;
use std::{
    fmt::{self, Display, Formatter},
    result,
};

pub type Result<T> = result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    JWTError(JWTError),
}

impl From<JWTError> for Error {
    fn from(err: JWTError) -> Self {
        Self::JWTError(err)
    }
}

impl Display for Error {
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
        write!(formatter, "{}", self)
    }
}
