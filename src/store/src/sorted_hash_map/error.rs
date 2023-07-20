use std::{result, fmt::Display};

use thiserror::Error;

pub type Result<T> = result::Result<T, Error>;

#[derive(Debug, Error)]
pub struct InternalError;

impl Display for InternalError{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("Internal error: {0}")]
    Internal(#[from] InternalError),
}



pub const ERR_TODO: InternalError = InternalError; //InternalError::new("00000", StatusCode::INTERNAL_SERVER_ERROR);
//
//pub const ERR_INTERNAL_CONTEXT_REQUIRED: InternalError =
//    InternalError::new("IN001", StatusCode::INTERNAL_SERVER_ERROR);
//
//pub const ERR_AUTH_LOG_IN_INVALID_PASSWORD: InternalError =
//    InternalError::new("AU001", StatusCode::FORBIDDEN);
//
//pub const ERR_ACCOUNT_CREATE_CONFLICT: InternalError =
//    InternalError::new("AC001", StatusCode::CONFLICT);
