use actix_http::body::BoxBody;
use actix_web::{
    error::ResponseError,
    http::{header, StatusCode},
    HttpResponse,
};
use datafusion::error::DataFusionError;
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
    DataFusionError(DataFusionError),
    Plan(String),
    Internal(InternalError),
    JWTError(jsonwebtoken::errors::Error),
    BincodeError(bincode::Error),
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

impl From<bincode::Error> for Error {
    fn from(err: bincode::Error) -> Self {
        Self::BincodeError(err)
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
            response = response.set_body(BoxBody::new(err.code));
        } else {
            println!("{}", self);
        }
        response
    }
}
