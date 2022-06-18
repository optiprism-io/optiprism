#[macro_use]
extern crate log;

extern crate bytesize;

mod error;

use crate::error::Error;
use axum::{Router, Server};
use bytesize::ByteSize;
use chrono::{DateTime, Utc};
use datafusion::datasource::MemTable;
use error::Result;
use events_gen::generator;
use log::info;
use metadata::{Metadata, Store};
use platform::platform::Platform;
use platform::{
    accounts::Provider as AccountProvider, auth::Provider as AuthProvider,
    events::Provider as EventsProvider, properties::Provider as PropertiesProvider,
};
use query::QueryProvider;
use std::env::temp_dir;
use std::path::PathBuf;
use std::{env::set_var, net::SocketAddr, sync::Arc};
use tower_http::add_extension::AddExtensionLayer;
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    // test env
    {
        set_var("FNP_COMMON_SALT", "FNP_COMMON_SALT");
        set_var("FNP_EMAIL_TOKEN_KEY", "FNP_EMAIL_TOKEN_KEY");
        set_var("FNP_ACCESS_TOKEN_KEY", "FNP_ACCESS_TOKEN_KEY");
        set_var("FNP_REFRESH_TOKEN_KEY", "FNP_REFRESH_TOKEN_KEY");
    }

    let mut path = temp_dir();
    path.push(format!("{}.db", Uuid::new_v4()));
    let store = Arc::new(Store::new(path));
    let md = Arc::new(Metadata::try_new(store)?);

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

    let provider = Arc::new(MemTable::try_new(batches[0][0].schema(), batches)?);
    let query_provider = Arc::new(QueryProvider::try_new(md.clone(), provider)?);
    let platform = platform::Platform::new(md.clone(), query_provider);

    let mut router = Router::new();
    router = platform::http::attach_routes(router, platform);

    let addr = SocketAddr::from(([127, 0, 0, 1], 8080));
    Server::bind(&addr)
        .serve(router.into_make_service())
        .await
        .map_err(|e| Error::ExternalError(e.to_string()));
    Ok(())
}
