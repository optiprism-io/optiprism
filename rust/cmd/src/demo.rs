extern crate bytesize;
extern crate log;

use std::env::temp_dir;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::{net::SocketAddr, sync::Arc};

use bytesize::ByteSize;
use chrono::{DateTime, Duration, Utc};
use datafusion::datasource::MemTable;
use log::{debug, error, info, warn};
use uuid::Uuid;

use crate::error::{Error, Result};
use metadata::store::Store;
use metadata::MetadataProvider;
use query::QueryProvider;
use rust_embed::RustEmbed;

pub struct Config {
    pub host: SocketAddr,
    pub md_path: PathBuf,
    pub from_date: DateTime<Utc>,
    pub to_date: DateTime<Utc>,
    pub new_daily_users: usize,
}

pub async fn run(cfg: Config) -> Result<()> {
    let store = Arc::new(Store::new(cfg.md_path.clone()));
    let md = Arc::new(MetadataProvider::try_new(store)?);

    info!("starting demo");
    info!("metadata data path: {:?}", cfg.md_path);
    info!("from date {}", cfg.from_date);
    let date_diff = cfg.to_date - cfg.from_date;
    info!("to date {}", cfg.to_date);
    info!(
        "time range: {}",
        humantime::format_duration(date_diff.to_std()?)
    );
    info!("new daily users: {}", cfg.new_daily_users);
    let total_users = cfg.new_daily_users as i64 * date_diff.num_days();
    info!("expecting total unique users: {total_users}");
    info!("starting sample data generation");

    let batches = {
        let sample_data_path = PathBuf::from(format!("{}/demo_data", env!("CARGO_MANIFEST_DIR")));
        let store_cfg = crate::store::Config {
            org_id: 1,
            project_id: 1,
            md: md.clone(),
            from_date: cfg.from_date,
            to_date: cfg.to_date,
            products_rdr: File::open(sample_data_path.join("products.csv"))
                .map_err(|err| Error::Internal(format!("can't open products.csv: {err}")))?,
            geo_rdr: File::open(sample_data_path.join("geo.csv"))
                .map_err(|err| Error::Internal(format!("can't open geo.csv: {err}")))?,
            device_rdr: File::open(sample_data_path.join("device.csv"))
                .map_err(|err| Error::Internal(format!("can't open device.csv: {err}")))?,
            new_daily_users: cfg.new_daily_users,
            batch_size: 4096,
            partitions: num_cpus::get(),
        };

        crate::store::gen(store_cfg).await?
    };

    info!("successfully generated");
    let mut rows: usize = 0;
    let mut data_size_bytes: usize = 0;
    for partition in batches.iter() {
        for batch in partition.iter() {
            rows += batch.num_rows();
            for column in batch.columns() {
                data_size_bytes += column.get_array_memory_size();
            }
        }
    }
    info!(
        "partitions: {}, batches: {}, rows: {rows}",
        batches.len(),
        batches[0].len()
    );
    info!("average {} event(s) per 1 user", rows as i64 / total_users);
    info!(
        "uncompressed dataset in-memory size: {}",
        ByteSize::b(data_size_bytes as u64)
    );

    let data_provider = Arc::new(MemTable::try_new(batches[0][0].schema(), batches)?);
    let query_provider = Arc::new(QueryProvider::try_new_from_provider(
        md.clone(),
        data_provider,
    )?);
    let platform_query_provider = Arc::new(platform::queries::provider::QueryProvider::new(
        query_provider,
    ));

    let pp = Arc::new(platform::PlatformProvider::new(
        md.clone(),
        platform_query_provider,
        Duration::days(1),
        "key".to_string(),
        Duration::days(1),
        "key".to_string(),
    ));
    let svc = platform::http::Service::new(&md, &pp, cfg.host.clone());
    info!("start listening on {}", cfg.host);
    svc.serve().await?;
    Ok(())
}
