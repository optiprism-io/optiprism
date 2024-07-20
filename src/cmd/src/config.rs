use std::net::SocketAddr;
use std::path::PathBuf;
use clap::ValueEnum;
use serde_derive::Deserialize;
use tracing::Level;
use tracing::level_filters::LevelFilter;

#[derive(Debug, Deserialize, PartialEq, Eq)]
pub struct Server {
    pub host: SocketAddr,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
pub struct Data {
    pub path: PathBuf,
    pub ui_path: PathBuf,
    pub ua_db_path: PathBuf,
    pub geo_city_path: PathBuf,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
pub struct Misc {
    pub session_cleaner_interval: String,
    pub project_default_session_duration: String,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
pub struct Auth {
    pub access_token_duration: String,
    pub refresh_token_duration: String,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
pub struct Log {
    pub level: LogLevel,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
pub struct Table {
    pub levels: usize,
    l0_max_parts: usize,
    l1_max_size_bytes: usize,
    level_size_multiplier: usize,
    max_log_length_bytes: usize,
    merge_max_l1_part_size_bytes: usize,
    merge_part_size_multiplier: usize,
    merge_data_page_size_limit_bytes: usize,
    merge_row_group_values_limit: usize,
    merge_array_size: usize,
    merge_chunk_size: usize,
    merge_array_page_size: usize,
    merge_max_page_size: usize,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
pub struct Config {
    pub server: Server,
    pub data: Data,
    pub misc: Misc,
    pub auth: Auth,
    pub events_table: Table,
    pub group_table: Table,
    pub log: Log,
}

fn parse_duration(s: &str) -> crate::error::Result<chrono::Duration> {
    Ok(chrono::Duration::from_std(parse_duration::parse(s)?)?)
}
impl TryInto<common::config::Config> for Config {
    type Error = crate::error::Error;

    fn try_into(self) -> Result<common::config::Config, Self::Error> {
        Ok(common::config::Config {
            server: common::config::Server {
                host: self.server.host,
            },
            data: common::config::Data {
                path: self.data.path,
                ua_db_path: self.data.ua_db_path,
                geo_city_path: self.data.geo_city_path,
                ui_path: self.data.ui_path,
            },
            auth: common::config::Auth {
                access_token_duration: parse_duration(self.auth.access_token_duration.as_str())?,
                refresh_token_duration: parse_duration(self.auth.refresh_token_duration.as_str())?,
                access_token_key: "".to_string(),
                refresh_token_key: "".to_string(),
            },
            log: common::config::Log { level: self.log.level.into() },
            misc: common::config::Misc {
                session_cleaner_interval: parse_duration(self.misc.session_cleaner_interval.as_str())?,
                project_default_session_duration: parse_duration(
                    self.misc.project_default_session_duration.as_str(),
                )?,
            },
            events_table: common::config::Table {
                levels: self.events_table.levels,
                l0_max_parts: self.events_table.l0_max_parts,
                l1_max_size_bytes: self.events_table.l1_max_size_bytes,
                level_size_multiplier: self.events_table.level_size_multiplier,
                max_log_length_bytes: self.events_table.max_log_length_bytes,
                merge_max_l1_part_size_bytes: self.events_table.merge_max_l1_part_size_bytes,
                merge_part_size_multiplier: self.events_table.merge_part_size_multiplier,
                merge_data_page_size_limit_bytes: self.events_table.merge_data_page_size_limit_bytes,
                merge_row_group_values_limit: self.events_table.merge_row_group_values_limit,
                merge_array_size: self.events_table.merge_array_size,
                merge_chunk_size: self.events_table.merge_chunk_size,
                merge_array_page_size: self.events_table.merge_array_page_size,
                merge_max_page_size: self.events_table.merge_max_page_size,
            },
            group_table: common::config::Table {
                levels: self.group_table.levels,
                l0_max_parts: self.group_table.l0_max_parts,
                l1_max_size_bytes: self.group_table.l1_max_size_bytes,
                level_size_multiplier: self.group_table.level_size_multiplier,
                max_log_length_bytes: self.group_table.max_log_length_bytes,
                merge_max_l1_part_size_bytes: self.group_table.merge_max_l1_part_size_bytes,
                merge_part_size_multiplier: self.group_table.merge_part_size_multiplier,
                merge_data_page_size_limit_bytes: self.group_table.merge_data_page_size_limit_bytes,
                merge_row_group_values_limit: self.group_table.merge_row_group_values_limit,
                merge_array_size: self.group_table.merge_array_size,
                merge_chunk_size: self.group_table.merge_chunk_size,
                merge_array_page_size: self.group_table.merge_array_page_size,
                merge_max_page_size: self.group_table.merge_max_page_size,
            }
        })
    }
}

#[derive(Deserialize, Copy, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
pub enum LogLevel {
    #[serde(rename = "trace")]
    Trace,
    #[serde(rename = "debug")]
    Debug,
    #[serde(rename = "info")]
    Info,
    #[serde(rename = "warn")]
    Warn,
    #[serde(rename = "error")]
    Error,
}

impl From<LogLevel> for LevelFilter {
    fn from(l: LogLevel) -> Self {
        match l {
            LogLevel::Trace => Level::TRACE,
            LogLevel::Debug => Level::DEBUG,
            LogLevel::Info => Level::INFO,
            LogLevel::Warn => Level::WARN,
            LogLevel::Error => Level::ERROR,
        }
            .into()
    }
}