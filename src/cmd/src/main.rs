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

use crate::error::Error;
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
        #[arg(long, default_value = "127.0.0.1:25535")]
        host: SocketAddr,
        #[arg(long)]
        demo_data_path: PathBuf,
        #[arg(long)]
        md_path: Option<PathBuf>,
        #[arg(long, default_value = ".")]
        ui_path: Option<PathBuf>,
        #[arg(long, default_value = "365 days")]
        duration: Option<String>,
        #[arg(long)]
        to_date: Option<String>,
        #[arg(long, default_value = "100")]
        new_daily_users: usize,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // a builder for `FmtSubscriber`.
    let subscriber = FmtSubscriber::builder()
        // all spans/events with a level higher than TRACE (e.g, debug, info, warn, etc.)
        // will be written to stdout.
        .with_max_level(cli.log_level)
        // completes the builder.
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
                Some(path) => {
                    if !path.is_dir() {
                        return Err(Error::Internal(
                            "md path should point to directory".to_string(),
                        ));
                    }

                    path
                }
            };

            if let Some(ui_path) = &ui_path {
                ui_path.try_exists()?;
            }

            let cfg = demo::Config {
                host,
                demo_data_path,
                md_path,
                ui_path,
                from_date,
                to_date,
                new_daily_users,
            };
            demo::run(cfg).await?
        }
    }

    Ok(())
}
