use std::net::AddrParseError;
use std::result;

use chrono::OutOfRangeError;
use demo::error::DemoError;
use thiserror::Error;

pub type Result<T> = result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Demo")]
    Demo(#[from] DemoError),
    #[error("IP Address Parse Error: {0:?}")]
    AddrParseError(#[from] AddrParseError),
    #[error("StdIO: {0:?}")]
    StdIO(#[from] std::io::Error),
    #[error("TimeDurationOutOfRange: {0:?}")]
    TimeDurationOutOfRange(#[from] OutOfRangeError),
    #[error("ParseDuration: {0:?}")]
    ParseDuration(#[from] parse_duration::parse::Error),
    #[error("other: {0:?}")]
    Other(#[from] anyhow::Error),
}
