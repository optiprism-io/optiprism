use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;

use chrono::Duration;
use serde::Deserialize;
use tracing::Level;
use tracing::level_filters::LevelFilter;

#[derive(Debug, Clone)]
pub struct Server {
    pub host: SocketAddr,
}
#[derive(Debug, Clone)]
pub struct Data {
    pub path: PathBuf,
    pub ua_db_path: PathBuf,
    pub geo_city_path: PathBuf,
    pub ui_path: Option<PathBuf>,

}
#[derive(Debug, Clone)]
pub struct Auth {
    pub access_token_duration: Duration,
    pub refresh_token_duration: Duration,
    pub access_token_key: String,
    pub refresh_token_key: String,
}
#[derive(Debug, Clone)]
pub struct Misc {
    pub session_cleaner_interval: Duration,
    pub project_default_session_duration: Duration,
}
#[derive(Debug, Clone)]
pub struct Log {
    pub level: LevelFilter,
}
#[derive(Debug, Clone)]
pub struct Config {
    pub server: Server,
    pub data: Data,
    pub auth: Auth,
    pub log: Log,
    pub misc: Misc,

}

impl Default for Config {
    fn default() -> Self {
        Config {
            server: Server {
                host: SocketAddr::from_str("0.0.0.0:8080").unwrap(),
            },
            data: Data {
                path: Default::default(),
                ua_db_path: Default::default(),
                geo_city_path: Default::default(),
                ui_path: None,
            },
            auth: Auth {
                access_token_duration: Default::default(),
                refresh_token_duration: Default::default(),
                access_token_key: "".to_string(),
                refresh_token_key: "".to_string(),
            },
            log: Log { level: LevelFilter::INFO },
            misc: Misc {
                session_cleaner_interval: Default::default(),
                project_default_session_duration: Default::default()
            },
        }
    }
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}