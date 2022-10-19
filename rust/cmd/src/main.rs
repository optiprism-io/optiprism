use crate::error::Result;
use chrono::{DateTime, Duration, Utc};
use clap::{Parser, Subcommand};
use dateparser::DateTimeUtc;
use log::info;
use std::env::temp_dir;
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
        #[arg(default_value = "127.0.0.1:25535")]
        host: String,
        md_path: Option<String>,
        #[arg(default_value = "365 days")]
        duration: Option<String>,
        to_date: Option<String>,
        #[arg(default_value = "100")]
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
                Some(v) => PathBuf::from(v),
            };

            let cfg = demo::Config {
                host: host.parse()?,
                md_path,
                from_date,
                to_date,
                new_daily_users,
            };
            demo::run(cfg).await?
        }
    }

    Ok(())
}
