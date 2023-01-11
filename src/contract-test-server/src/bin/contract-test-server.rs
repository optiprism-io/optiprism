use std::env::temp_dir;
use std::fs::File;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use chrono::Duration;
use chrono::Utc;
use clap::Parser;
use clap::ValueEnum;
use datafusion::datasource::MemTable;
use tracing::metadata::LevelFilter;
use tracing::{debug, info, Level};
use metadata::MetadataProvider;
use metadata::store::Store;
use platform::auth;
use service::tracing::TracingCliArgs;


#[derive(Parser)]
#[command(propagate_version = true)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[clap(flatten)]
    tracing: TracingCliArgs,
    #[arg(long, default_value = "0.0.0.0:8080")]
    host: SocketAddr,
    #[arg(long, default_value = "refresh_token_key")]
    refresh_token_key: String,
    #[arg(long, default_value = "access_token_key")]
    access_token_key: String,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let args = Args::parse();
    args.tracing.init()?;

    info!("starting http stubs instance...");

    let md = Arc::new(MetadataProvider::new_stub());

    let auth_cfg = auth::Config {
        access_token_duration: Duration::days(1),
        access_token_key: args.access_token_key,
        refresh_token_duration: Duration::days(1),
        refresh_token_key: args.refresh_token_key,
    };

    let platform_provider = Arc::new(platform::PlatformProvider::new_stub());
    let svc = platform::http::Service::new(&md, &platform_provider, auth_cfg, args.host);

    info!("start listening on {}", args.host);

    Ok(svc.serve().await?)
}