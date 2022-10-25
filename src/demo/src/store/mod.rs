use std::collections::HashMap;
use std::io;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use chrono::DateTime;
use chrono::Utc;
use enum_iterator::all;
use events_gen::generator;
use events_gen::generator::Generator;
use events_gen::profiles::ProfileProvider;
use metadata::MetadataProvider;
use rand::thread_rng;
use tracing::info;

use crate::error::Result;
use crate::store::events::Event;
use crate::store::products::ProductProvider;
use crate::store::scenario::Scenario;
use crate::store::schema::create_entities;

pub mod actions;
mod batch_builder;
mod coefficients;
pub mod events;
mod intention;
mod products;
pub mod scenario;
mod schema;
mod transitions;

pub struct Config<R> {
    pub org_id: u64,
    pub project_id: u64,
    pub md: Arc<MetadataProvider>,
    pub from_date: DateTime<Utc>,
    pub to_date: DateTime<Utc>,
    pub products_rdr: R,
    pub geo_rdr: R,
    pub device_rdr: R,
    pub new_daily_users: usize,
    pub batch_size: usize,
    pub partitions: usize,
}

pub async fn gen<R>(cfg: Config<R>) -> Result<Vec<Vec<RecordBatch>>>
where R: io::Read {
    let mut rng = thread_rng();

    info!("loading profiles...");
    let profiles = ProfileProvider::try_new_from_csv(
        cfg.org_id,
        cfg.project_id,
        &cfg.md.dictionaries,
        cfg.geo_rdr,
        cfg.device_rdr,
    )?;
    info!("loading products...");
    let products = ProductProvider::try_new_from_csv(
        cfg.org_id,
        cfg.project_id,
        &mut rng,
        cfg.md.dictionaries.clone(),
        cfg.products_rdr,
    )
    .await?;
    info!("creating entities...");
    let schema = Arc::new(create_entities(cfg.org_id, cfg.project_id, &cfg.md).await?);

    info!("creating generator...");
    let gen_cfg = generator::Config {
        rng: rng.clone(),
        profiles,
        from: cfg.from_date,
        to: cfg.to_date,
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

    info!("generating events...");
    let run_cfg = scenario::Config {
        rng: rng.clone(),
        gen,
        schema: schema.clone(),
        events_map,
        products,
        to: cfg.to_date,
        batch_size: cfg.batch_size,
        partitions: cfg.partitions,
    };

    let mut scenario = Scenario::new(run_cfg);
    let result = scenario.run().await?;

    Ok(result)
}
