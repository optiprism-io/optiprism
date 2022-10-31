extern crate bytesize;

use std::env::temp_dir;
use std::fs::File;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use bytesize::ByteSize;
use chrono::DateTime;
use chrono::Duration;
use chrono::Utc;
use datafusion::arrow::ipc::Duration;
use datafusion::datasource::MemTable;
use metadata::store::Store;
use metadata::MetadataProvider;
use platform::auth;
use query::QueryProvider;
use tracing::debug;
use tracing::info;

use crate::error::DemoError;
use crate::error::Result;

pub struct Config {
    pub host: SocketAddr,
}

pub async fn run(host: SocketAddr) -> anyhow::Result<()> {
    let store = Arc::new(Store::new(
        temp_dir().join(format!("{}.db", Uuid::new_v4())),
    ));
    let md = Arc::new(MetadataProvider::try_new(store)?);

    info!("starting test contract instance...");
    let data_provider = Arc::new(MemTable::ne(batches[0][0].schema(), batches)?);
    let query_provider = Arc::new(QueryProvider::try_new_from_provider(
        md.clone(),
        data_provider,
    )?);

    let auth_cfg = auth::Config {
        access_token_duration: Duration::days(1),
        access_token_key: "access".to_owned(),
        refresh_token_duration: Duration::days(1),
        refresh_token_key: "refresh".to_owned(),
    };

    let platform_provider = Arc::new(platform::PlatformProvider::new(
        md.clone(),
        query_provider,
        auth_cfg.clone(),
    ));

    let svc = platform::http::Service::new(
        &md,
        &platform_provider,
        auth_cfg,
        cfg.host,
        cfg.ui_path.clone(),
    );
    info!("start listening on {}", cfg.host);
    if cfg.ui_path.is_some() {
        info!("http ui http://{}", cfg.host);
    }
    svc.serve().await?;
    Ok(())
}
