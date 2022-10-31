use std::env::temp_dir;
use std::net::SocketAddr;
use std::path::PathBuf;

use chrono::Duration;
use chrono::Utc;
use clap::Parser;
use clap::Subcommand;
use clap::ValueEnum;
use dateparser::DateTimeUtc;
use tracing::metadata::LevelFilter;
use tracing::Level;
use tracing_subscriber::FmtSubscriber;
use uuid::Uuid;

use crate::error::Result;

extern crate parse_duration;

mod contract_test;
mod demo;
mod error;

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
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

#[derive(Parser)]
#[command(propagate_version = true)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[arg(value_enum, default_value = "debug")]
    log_level: LogLevel,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    Demo(demo::Config),
    ContractTest(contract_test::Config),
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let subscriber = FmtSubscriber::builder()
        .with_max_level(cli.log_level)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    match cli.cmd {
        Cmd::Demo(cfg) => demo::run(cfg).await?,
        Cmd::ContractTest(cfg) => {}
    }

    Ok(())
}
