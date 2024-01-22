use std::net::SocketAddr;
use std::path::PathBuf;

use serde_derive::Deserialize;

#[derive(Debug, Deserialize, PartialEq, Eq)]
pub struct Config {
    pub path: PathBuf,
    pub host: SocketAddr,
    pub ui_path: Option<PathBuf>,
    pub ua_db_path: PathBuf,
    pub geo_city_path: PathBuf,
}
