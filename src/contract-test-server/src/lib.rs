mod metadata_stub;
mod platform_stub;

use std::net::SocketAddr;
use std::sync::Arc;

use chrono::DateTime;
use chrono::Duration;
use chrono::NaiveDateTime;
use chrono::Utc;
use lazy_static::lazy_static;
use platform::auth;
use tracing::info;

lazy_static! {
    pub static ref DATE_TIME: DateTime<Utc> =
        DateTime::from_utc(NaiveDateTime::from_timestamp(1000, 0), Utc);
}

pub struct Config {
    pub host: SocketAddr,
    pub refresh_token_key: String,
    pub access_token_key: String,
}

pub async fn run(cfg: Config) -> anyhow::Result<()> {
    info!("starting http stubs instance...");

    let md = Arc::new(metadata::MetadataProvider {
        events: Arc::new(metadata_stub::Events {}),
        custom_events: Arc::new(metadata_stub::CustomEvents {}),
        event_properties: Arc::new(metadata_stub::Properties {}),
        user_properties: Arc::new(metadata_stub::Properties {}),
        organizations: Arc::new(metadata_stub::Organizations {}),
        projects: Arc::new(metadata_stub::Projects {}),
        accounts: Arc::new(metadata_stub::Accounts {}),
        database: Arc::new(metadata_stub::Database {}),
        dictionaries: Arc::new(metadata_stub::Dictionaries {}),
    });

    let auth_cfg = auth::Config {
        access_token_duration: Duration::days(1),
        access_token_key: cfg.access_token_key,
        refresh_token_duration: Duration::days(1),
        refresh_token_key: cfg.refresh_token_key,
    };

    let platform_provider = Arc::new(platform::PlatformProvider {
        events: Arc::new(platform_stub::Events {}),
        custom_events: Arc::new(platform_stub::CustomEvents {}),
        event_properties: Arc::new(platform_stub::Properties {}),
        user_properties: Arc::new(platform_stub::Properties {}),
        accounts: Arc::new(platform_stub::Accounts {}),
        auth: Arc::new(platform_stub::Auth {}),
        query: Arc::new(platform_stub::Queries {}),
    });
    let svc = platform::http::Service::new(&md, &platform_provider, auth_cfg, cfg.host, None);
    info!("start listening on {}", cfg.host);
    svc.serve().await?;
    Ok(())
}
