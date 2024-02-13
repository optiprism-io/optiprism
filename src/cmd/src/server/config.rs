use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use serde_derive::Deserialize;

#[derive(Debug, Deserialize, PartialEq, Eq)]
pub struct Config {
    pub path: PathBuf,
    pub host: SocketAddr,
    pub ui_path: Option<PathBuf>,
    pub ua_db_path: PathBuf,
    pub geo_city_path: PathBuf,
    pub session_cleaner_interval: String,
    pub project_default_session_duration: String,
    pub access_token_duration: String,
    pub refresh_token_duration: String,
}

fn parse_duration(s: &str) -> crate::error::Result<chrono::Duration> {
    Ok(chrono::Duration::from_std(parse_duration::parse(s)?)?)
}
impl TryInto<common::config::Config> for Config {
    type Error = crate::error::Error;

    fn try_into(self) -> Result<common::config::Config, Self::Error> {
        Ok(common::config::Config {
            path: self.path,
            host: self.host,
            ui_path: self.ui_path,
            ua_db_path: self.ua_db_path,
            geo_city_path: self.geo_city_path,
            session_cleaner_interval: parse_duration(self.session_cleaner_interval.as_str())?,
            project_default_session_duration: parse_duration(
                self.project_default_session_duration.as_str(),
            )?,
            access_token_duration: parse_duration(self.access_token_duration.as_str())?,
            refresh_token_duration: parse_duration(self.refresh_token_duration.as_str())?,
        })
    }
}
