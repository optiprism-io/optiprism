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
    Demo {
        #[arg(long, default_value = "0.0.0.0:8080")]
        host: SocketAddr,
        #[arg(long)]
        demo_data_path: PathBuf,
        #[arg(long)]
        md_path: Option<PathBuf>,
        #[arg(long)]
        ui_path: Option<PathBuf>,
        #[arg(long, default_value = "365 days")]
        duration: Option<String>,
        #[arg(long)]
        to_date: Option<String>,
        #[arg(long, default_value = "10")]
        new_daily_users: usize,
    },
    ContractTestServer {
        #[arg(long, default_value = "0.0.0.0:8080")]
        host: SocketAddr,
        #[arg(long, default_value = "refresh_token_key")]
        refresh_token_key: String,
        #[arg(long, default_value = "access_token_key")]
        access_token_key: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let subscriber = FmtSubscriber::builder()
        .with_max_level(cli.log_level)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    match cli.cmd {
        Cmd::Demo {
            host,
            demo_data_path,
            md_path,
            ui_path,
            duration,
            to_date,
            new_daily_users,
        } => {
            let to_date = match to_date {
                None => Utc::now(),
                Some(dt) => dt.parse::<DateTimeUtc>()?.0.with_timezone(&Utc),
            };

            let duration = Duration::from_std(parse_duration::parse(duration.unwrap().as_str())?)?;
            let from_date = to_date - duration;
            let md_path = match md_path {
                None => temp_dir().join(format!("{}.db", Uuid::new_v4())),
                Some(path) => path,
            };

            md_path.try_exists()?;

            if let Some(ui_path) = &ui_path {
                ui_path.try_exists()?;
            }

            demo_data_path.try_exists()?;

            let cfg = demo::Config {
                host,
                demo_data_path,
                md_path,
                ui_path,
                from_date,
                to_date,
                new_daily_users,
            };

            demo::run(cfg).await?;
        }
        Cmd::ContractTestServer {
            host,
            refresh_token_key,
            access_token_key,
        } => {
            let cfg = contract_test_server::Config {
                host,
                refresh_token_key,
                access_token_key,
            };

            contract_test_server::run(cfg).await?;
        }
    }
    Ok(())
}
