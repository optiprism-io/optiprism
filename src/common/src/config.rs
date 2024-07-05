use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;

use chrono::Duration;

#[derive(Debug, Clone)]
pub struct Config {
    pub path: PathBuf,
    pub host: SocketAddr,
    pub ui_path: Option<PathBuf>,
    pub ua_db_path: PathBuf,
    pub geo_city_path: PathBuf,
    pub session_cleaner_interval: Duration,
    pub project_default_session_duration: Duration,
    pub access_token_key: String,
    pub refresh_token_key: String,
    pub access_token_duration: Duration,
    pub refresh_token_duration: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            path: Default::default(),
            host: SocketAddr::from_str("0.0.0.0:8080").unwrap(),
            ui_path: None,
            ua_db_path: Default::default(),
            geo_city_path: Default::default(),
            session_cleaner_interval: Duration::seconds(1),
            project_default_session_duration: Duration::days(1),
            access_token_key: Default::default(),
            refresh_token_key: Default::default(),
            access_token_duration: Duration::hours(1),
            refresh_token_duration: Duration::days(1),
        }
    }
}
