use std::path::Path;
use std::sync::Arc;
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use rand::thread_rng;
use metadata::{dictionaries, Metadata};
use crate::generator::Generator;
use crate::profiles::{Profile, ProfileProvider};
use crate::store::events::Event;
use crate::store::products::ProductProvider;
use crate::store::scenario::run;
use crate::store::schema::create_entities;
use crate::error::Result;

mod products;
mod scenario;
mod schema;
mod batch_builder;
mod events;
mod actions;
mod intention;
mod coefficients;
mod transitions;

pub struct Config {
    pub org_id: u64,
    pub project_id: u64,
    pub md: Arc<Metadata>,
    pub dicts: Arc<dictionaries::Provider>,
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
    pub products_path: Path,
    pub geo_path: Path,
    pub device_path: Path,
    pub new_daily_users: usize,
    pub batch_size: usize,
    pub partitions: usize,
}

pub fn gen(cfg: Config) -> Result<Vec<Vec<RecordBatch>>> {
    let mut rng = thread_rng();

    let mut products = ProductProvider::try_new_from_csv(cfg.products_path, &mut rng, cfg.org_id, cfg.project_id, &cfg.dicts).unwrap();
    let mut profiles = ProfileProvider::try_new_from_csv(cfg.org_id, cfg.project_id, &cfg.dicts, cfg.geo_path, cfg.device_path)?;

    let hour_weights = [0.4, 0.37, 0.39, 0.43, 0.45, 0.47, 0.52, 0.6, 0.8, 0.9, 0.85, 0.8, 0.75, 0.85, 1., 0.85, 0.7, 0.63, 0.62, 0.61, 0.59, 0.57, 0.48, 0.4];
    let mut gen = Generator::new(rng.clone(), profiles, cfg.from, cfg.to.clone(), cfg.new_daily_users, &hour_weights);

    let schema = create_entities(&cfg.md, cfg.org_id, cfg.project_id)?;

    let events_map = Event::into_enum_iter()
        .map(|v| cfg.md.events.get_by_name(cfg.org_id, cfg.project_id, v.as_str()))
        .collect();

    run(rng, &mut gen, schema, events_map, &mut products, cfg.to, cfg.batch_size, cfg.partitions)
}