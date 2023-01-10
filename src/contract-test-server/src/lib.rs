use std::net::SocketAddr;
use std::sync::Arc;

use chrono::Duration;
use platform::auth;
use tracing::info;

pub struct Config {
    pub host: SocketAddr,
    pub refresh_token_key: String,
    pub access_token_key: String,
}

pub async fn run(cfg: Config) -> anyhow::Result<()> {
    info!("starting http stubs instance...");

    let md = Arc::new(metadata::MetadataProvider::new_stub());

    let auth_cfg = auth::Config {
        access_token_duration: Duration::days(1),
        access_token_key: cfg.access_token_key,
        refresh_token_duration: Duration::days(1),
        refresh_token_key: cfg.refresh_token_key,
    };

    let platform_provider = Arc::new(platform::PlatformProvider::new_stub());
    let svc = platform::http::Service::new(&md, &platform_provider, auth_cfg, cfg.host);

    info!("start listening on {}", cfg.host);
    Ok(svc.serve().await?)
}
