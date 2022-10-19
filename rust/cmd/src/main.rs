use crate::error::{Error, Result};
use chrono::{DateTime, Duration, Utc};
use clap::{Parser, Subcommand};
use dateparser::DateTimeUtc;
use log::info;
use std::borrow::BorrowMut;
use std::env::temp_dir;
use std::net::SocketAddr;
use std::path::PathBuf;
use uuid::Uuid;

extern crate parse_duration;

mod demo;
mod error;
mod store;

#[derive(Parser)]
#[command(propagate_version = true)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    Demo {
        #[arg(long, default_value = "127.0.0.1:25535")]
        host: SocketAddr,
        #[arg(long)]
        md_path: Option<PathBuf>,
        #[arg(long, default_value = ".")]
        ui_path: PathBuf,
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
    env_logger::builder().format_target(false).init();
    let cmd = Cli::parse();
    match cmd.cmd {
        Cmd::Demo {
            host,
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
                    if !path.is_file() {
                        return Err(Error::Internal("md path should point to file".to_string()));
                    }

                    let mut check = path.clone();
                    check.pop();
                    check.try_exists()?;
                    path
                }
            };

            ui_path.try_exists()?;

            let cfg = demo::Config {
                host,
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
