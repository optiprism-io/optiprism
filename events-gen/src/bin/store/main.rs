use std::path::PathBuf;
use std::sync::Arc;
use chrono::{DateTime, Utc};
use rand::thread_rng;
use events_gen::generator::Generator;
use events_gen::store::products::{Preferences, ProductProvider};
use events_gen::store::scenario::{RecordBatchBuilder, run};
use metadata::database::{Column, TableType};
use metadata::{events, Metadata};
use metadata::properties::{CreatePropertyRequest, Property};
use metadata::properties::provider::Namespace;
use events_gen::error::Result;
use events_gen::store;


#[tokio::main]
async fn main() -> Result<()> {
    let mut rng = thread_rng();
    let root_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));

    let mut products_path = root_path.clone();
    products_path.push("src/store/data/products.csv");

    let mut geo_path = root_path.clone();
    geo_path.push("src/data/geo.csv");

    let mut device_path = root_path.clone();
    device_path.push("src/data/device.csv");

    cfg = store::Config{
        org_id: 0,
        project_id: 0,
        from: (),
        to: (),
        products_path: (),
        geo_path: (),
        device_path: (),
        new_daily_users: 0,
        batch_size: 0,
        partitions: 0
    }
    let mut products = ProductProvider::try_new_from_csv(products_path, &mut rng).unwrap();

    let from = DateTime::parse_from_rfc3339("2021-09-08T13:42:00.000000+00:00").unwrap().with_timezone(&Utc);
    let to = DateTime::parse_from_rfc3339("2022-09-08T14:42:00.000000+00:00").unwrap().with_timezone(&Utc);
    let hour_weights = [0.4, 0.37, 0.39, 0.43, 0.45, 0.47, 0.52, 0.6, 0.8, 0.9, 0.85, 0.8, 0.75, 0.85, 1., 0.85, 0.7, 0.63, 0.62, 0.61, 0.59, 0.57, 0.48, 0.4];
    let mut gen = Generator::new(
        rng.clone(),
        from,
        to.clone(),
        30,
        &hour_weights,
    );

    create_entities()
    run(rng, &mut gen, &mut products, to, 10_000)
}