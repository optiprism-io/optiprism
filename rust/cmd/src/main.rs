extern crate bytesize;
extern crate log;

use std::env::temp_dir;
use std::path::PathBuf;
use std::{net::SocketAddr, sync::Arc};

use axum::{Router, Server};
use bytesize::ByteSize;
use chrono::{DateTime, Duration, Utc};
use datafusion::datasource::MemTable;
use log::info;
use tower_cookies::CookieManagerLayer;
use uuid::Uuid;

use error::Result;
use metadata::store::Store;
use metadata::MetadataProvider;
use query::QueryProvider;

use crate::error::Error;

mod error;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let mut path = temp_dir();
    path.push(format!("{}.db", Uuid::new_v4()));
    let store = Arc::new(Store::new(path));
    let md = Arc::new(MetadataProvider::try_new(store)?);

    info!("starting sample data generation");
    let batches = {
        let root_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));

        let mut products_path = root_path.clone();
        products_path.push("../events-gen/src/store/data/products.csv");

        let mut geo_path = root_path.clone();
        geo_path.push("../events-gen/src/data/geo.csv");

        let mut device_path = root_path.clone();
        device_path.push("../events-gen/src/data/device.csv");

        let cfg = events_gen::store::Config {
            org_id: 1,
            project_id: 1,
            md: md.clone(),
            from: DateTime::parse_from_rfc3339("2021-09-08T13:42:00.000000+00:00")
                .unwrap()
                .with_timezone(&Utc),
            to: DateTime::parse_from_rfc3339("2022-09-08T14:42:00.000000+00:00")
                .unwrap()
                .with_timezone(&Utc),
            products_path,
            geo_path,
            device_path,
            new_daily_users: 1,
            batch_size: 4096,
            partitions: num_cpus::get(),
        };

        events_gen::store::gen(cfg).await?
    };

    println!("successfully generated");
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
    println!(
        "partitions: {}, batches: {}, rows: {rows}",
        batches.len(),
        batches[0].len()
    );
    println!("total size: {}", ByteSize::b(data_size_bytes as u64));

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

    let addr = SocketAddr::from(([127, 0, 0, 1], 8080));
    let svc = platform::http::Service::new(&md, &pp, addr);

    svc.serve().await?;
    Ok(())
}
