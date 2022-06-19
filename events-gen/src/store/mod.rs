use crate::error::Result;
use crate::generator;
use crate::generator::Generator;
use crate::profiles::ProfileProvider;
use crate::store::events::Event;
use crate::store::products::ProductProvider;
use crate::store::scenario::Scenario;
use crate::store::schema::create_entities;
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use enum_iterator::all;

use log::info;
use metadata::Metadata;
use rand::thread_rng;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

pub mod actions;
mod batch_builder;
mod coefficients;
pub mod events;
mod intention;
mod products;
pub mod scenario;
mod schema;
mod transitions;

pub struct Config {
    pub org_id: u64,
    pub project_id: u64,
    pub md: Arc<Metadata>,
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
    pub products_path: PathBuf,
    pub geo_path: PathBuf,
    pub device_path: PathBuf,
    pub new_daily_users: usize,
    pub batch_size: usize,
    pub partitions: usize,
}

pub async fn gen(cfg: Config) -> Result<Vec<Vec<RecordBatch>>> {
    let mut rng = thread_rng();

    info!("loading profiles");
    let profiles = ProfileProvider::try_new_from_csv(
        cfg.org_id,
        cfg.project_id,
        &cfg.md.dictionaries,
        cfg.geo_path,
        cfg.device_path,
    )?;
    info!("loading products");
    let products = ProductProvider::try_new_from_csv(
        cfg.org_id,
        cfg.project_id,
        &mut rng,
        cfg.md.dictionaries.clone(),
        cfg.products_path,
    )
    .await?;
    info!("creating entities");
    let schema = Arc::new(create_entities(cfg.org_id, cfg.project_id, &cfg.md).await?);

    info!("creating generator");
    let gen_cfg = generator::Config {
        rng: rng.clone(),
        profiles,
        from: cfg.from,
        to: cfg.to,
        new_daily_users: cfg.new_daily_users,
        traffic_hourly_weights: [
            0.4, 0.37, 0.39, 0.43, 0.45, 0.47, 0.52, 0.6, 0.8, 0.9, 0.85, 0.8, 0.75, 0.85, 1.,
            0.85, 0.7, 0.63, 0.62, 0.61, 0.59, 0.57, 0.48, 0.4,
        ],
    };

    let gen = Generator::new(gen_cfg);

    let mut events_map: HashMap<Event, u64> = HashMap::default();
    for event in all::<Event>() {
        let md_event = cfg
            .md
            .events
            .get_by_name(cfg.org_id, cfg.project_id, event.to_string().as_str())
            .await?;
        events_map.insert(event, md_event.id);
    }

    info!("generating");
    let run_cfg = scenario::Config {
        rng: rng.clone(),
        gen,
        schema: schema.clone(),
        events_map,
        products,
        to: cfg.to,
        batch_size: cfg.batch_size,
        partitions: cfg.partitions,
    };

    let mut scenario = Scenario::new(run_cfg);
    let result = scenario.run().await?;

    Ok(result)
}
