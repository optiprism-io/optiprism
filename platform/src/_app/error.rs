use actix_http::body::AnyBody;
use actix_web::{
    error::ResponseError,
    http::{header, StatusCode},
    HttpResponse,
};
use std::{
    fmt::{self, Display, Formatter},
    result,
};

pub type Result<T> = result::Result<T, Error>;

#[derive(Debug, Clone)]
pub struct InternalError {
    code: &'static str,
    status_code: StatusCode,
}

impl InternalError {
    const fn new(code: &'static str, status_code: StatusCode) -> Self {
        Self { code, status_code }
    }
}

#[derive(Debug)]
pub enum Error {
    Internal(InternalError),
    JWTError(jsonwebtoken::errors::Error),
}

impl Display for Error {
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
        write!(formatter, "{}", self)
    }
}

impl From<jsonwebtoken::errors::Error> for Error {
    fn from(err: jsonwebtoken::errors::Error) -> Self {
        Self::JWTError(err)
    }
}

impl From<InternalError> for Error {
    fn from(err: InternalError) -> Self {
        Self::Internal(err)
    }
}

impl ResponseError for Error {
    fn status_code(&self) -> StatusCode {
        if let Error::Internal(err) = self {
            return err.status_code;
        }
        StatusCode::INTERNAL_SERVER_ERROR
    }

    fn error_response(&self) -> HttpResponse {
        let mut response = HttpResponse::new(self.status_code());
        response.headers_mut().insert(
            header::CONTENT_TYPE,
            header::HeaderValue::from_static("text/plain; charset=utf-8"),
        );
        if let Error::Internal(err) = self {
            response = response.set_body(AnyBody::from(err.code));
        } else {
            println!("{}", self);
        }
        response
    }
}

pub const ERR_TODO: InternalError = InternalError::new("00000", StatusCode::INTERNAL_SERVER_ERROR);

pub const ERR_INTERNAL_CONTEXT_REQUIRED: InternalError =
    InternalError::new("IN001", StatusCode::INTERNAL_SERVER_ERROR);

// Auth error
pub const ERR_AUTH_LOG_IN_INVALID_PASSWORD: InternalError =
    InternalError::new("AU001", StatusCode::FORBIDDEN);

// Account error
pub const ERR_ACCOUNT_CREATE_CONFLICT: InternalError =
    InternalError::new("AC001", StatusCode::CONFLICT);
pub const ERR_ACCOUNT_NOT_FOUND: InternalError = InternalError::new("AC002", StatusCode::NOT_FOUND);

// Organization error
pub const ERR_ORGANIZATION_CREATE_CONFLICT: InternalError =
    InternalError::new("OR001", StatusCode::CONFLICT);
pub const ERR_ORGANIZATION_NOT_FOUND: InternalError =
    InternalError::new("OR002", StatusCode::NOT_FOUND);
