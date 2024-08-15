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
    pub ui_path: PathBuf,
}

#[derive(Debug, Clone)]
pub struct Auth {
    pub access_token_duration: Duration,
    pub refresh_token_duration: Duration,
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
pub struct Table {
    pub levels: usize,
    pub l0_max_parts: usize,
    pub l1_max_size_bytes: usize,
    pub level_size_multiplier: usize,
    pub max_log_length_bytes: usize,
    pub merge_max_l1_part_size_bytes: usize,
    pub merge_part_size_multiplier: usize,
    pub merge_data_page_size_limit_bytes: usize,
    pub merge_row_group_values_limit: usize,
    pub merge_array_size: usize,
    pub merge_chunk_size: usize,
    pub merge_array_page_size: usize,
    pub merge_max_page_size: usize,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub server: Server,
    pub data: Data,
    pub auth: Auth,
    pub misc: Misc,
    pub events_table: Table,
    pub group_table: Table,
    pub log: Log,
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
                ui_path: Default::default()
            },
            auth: Auth {
                access_token_duration: Default::default(),
                refresh_token_duration: Default::default(),
            },
            log: Log { level: LevelFilter::INFO },
            misc: Misc {
                session_cleaner_interval: Default::default(),
                project_default_session_duration: Default::default()
            },
            events_table: Table {
                levels: 0,
                l0_max_parts: 0,
                l1_max_size_bytes: 0,
                level_size_multiplier: 0,
                max_log_length_bytes: 0,
                merge_max_l1_part_size_bytes: 0,
                merge_part_size_multiplier: 0,
                merge_data_page_size_limit_bytes: 0,
                merge_row_group_values_limit: 0,
                merge_array_size: 0,
                merge_chunk_size: 0,
                merge_array_page_size: 0,
                merge_max_page_size: 0,
            },
            group_table: Table {
                levels: 0,
                l0_max_parts: 0,
                l1_max_size_bytes: 0,
                level_size_multiplier: 0,
                max_log_length_bytes: 0,
                merge_max_l1_part_size_bytes: 0,
                merge_part_size_multiplier: 0,
                merge_data_page_size_limit_bytes: 0,
                merge_row_group_values_limit: 0,
                merge_array_size: 0,
                merge_chunk_size: 0,
                merge_array_page_size: 0,
                merge_max_page_size: 0,
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