use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use common::Error as CommonError;
use geoip2::Error as GeoIP2Error;
use metadata::Error as MetadataError;
use std::{
    fmt::{self, Display, Formatter},
    io::Error as IOError,
    result,
};

pub type Result<T> = result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    IOError(IOError),
    CommonError(CommonError),
    MetadataError(MetadataError),
    GeoIP2Error(GeoIP2Error),
}

impl Display for Error {
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
        write!(formatter, "{}", self)
    }
}

impl From<IOError> for Error {
    fn from(err: IOError) -> Self {
        Self::IOError(err)
    }
}

impl From<CommonError> for Error {
    fn from(err: CommonError) -> Self {
        Self::CommonError(err)
    }
}

impl From<MetadataError> for Error {
    fn from(err: MetadataError) -> Self {
        Self::MetadataError(err)
    }
}

impl From<GeoIP2Error> for Error {
    fn from(err: GeoIP2Error) -> Self {
        Self::GeoIP2Error(err)
    }
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        (StatusCode::INTERNAL_SERVER_ERROR, format!("{}", self)).into_response()
    }
}
