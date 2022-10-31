use std::env::temp_dir;
use std::net::SocketAddr;
use std::path::PathBuf;

use chrono::Utc;
use clap::Args;
use dateparser::DateTimeUtc;
use time::Duration;
use uuid::Uuid;

use crate::Result;

#[derive(Args)]
pub struct Config {
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
    #[arg(long, default_value = "100")]
    new_daily_users: usize,
}

pub async fn run(cfg: Config) -> Result<()> {
    let to_date = match cfg.to_date {
        None => Utc::now(),
        Some(dt) => dt.parse::<DateTimeUtc>()?.0.with_timezone(&Utc),
    };

    let duration = Duration::from_std(parse_duration::parse(cfg.duration.unwrap().as_str())?)?;
    let from_date = to_date - duration;
    let md_path = match cfg.md_path {
        None => temp_dir().join(format!("{}.db", Uuid::new_v4())),
        Some(path) => path,
    };

    md_path.try_exists()?;

    if let Some(ui_path) = &cfg.ui_path {
        ui_path.try_exists()?;
    }

    cfg.demo_data_path.try_exists()?;

    let cfg = demo::Config {
        host: cfg.host,
        demo_data_path: cfg.demo_data_path,
        md_path,
        ui_path: cfg.ui_path,
        from_date,
        to_date,
        new_daily_users: cfg.new_daily_users,
    };

    Ok(demo::run(cfg).await?)
}
