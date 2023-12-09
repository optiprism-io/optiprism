use std::collections::HashMap;
use std::{io, thread};
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use chrono::DateTime;
use chrono::Utc;
use crossbeam_channel::bounded;
use enum_iterator::all;
use events_gen::generator;
use events_gen::generator::Generator;
use events_gen::store::events::Event;
use events_gen::store::products::ProductProvider;
use events_gen::store::profiles::ProfileProvider;
use events_gen::store::scenario;
use events_gen::store::scenario::Scenario;
use events_gen::store::schema::create_entities;
use futures::executor::block_on;
use metadata::MetadataProvider;
use rand::thread_rng;
use tracing::info;
use store::db::{OptiDBImpl, TableOptions};
use store::{NamedValue, Value};

pub struct Config<R> {
    pub org_id: u64,
    pub project_id: u64,
    pub from_date: DateTime<Utc>,
    pub to_date: DateTime<Utc>,
    pub products_rdr: R,
    pub geo_rdr: R,
    pub device_rdr: R,
    pub new_daily_users: usize,
    pub batch_size: usize,
    pub partitions: usize,
}

pub fn gen<R>(
    md: &Arc<MetadataProvider>,
    db: &Arc<OptiDBImpl>,
    cfg: Config<R>,
) -> Result<(), anyhow::Error>
    where
        R: io::Read,
{
    let mut rng = thread_rng();
    info!("creating entities...");

    let topts = TableOptions {
        levels: 7,
        merge_array_size: 10000,
        partitions: cfg.partitions,
        index_cols: 2,
        l1_max_size_bytes: 1024 * 1024 * 10,
        level_size_multiplier: 10,
        l0_max_parts: 4,
        max_log_length_bytes: 1024 * 1024*10,
        merge_array_page_size: 10000,
        merge_data_page_size_limit_bytes: Some(1024 * 1024),
        merge_index_cols: 2,
        merge_max_l1_part_size_bytes: 1024 * 1024,
        merge_part_size_multiplier: 10,
        merge_row_group_values_limit: 1000,
        merge_chunk_size: 1024 * 8 * 8,
    };
    db.create_table("events", topts)?;
    create_entities(cfg.org_id, cfg.project_id, md, db)?;
    info!("loading profiles...");
    let profiles = ProfileProvider::try_new_from_csv(
        cfg.org_id,
        cfg.project_id,
        &md.dictionaries,
        cfg.geo_rdr,
        cfg.device_rdr,
    )?;
    info!("loading products...");
    let products = ProductProvider::try_new_from_csv(
        cfg.org_id,
        cfg.project_id,
        &mut rng,
        md.dictionaries.clone(),
        cfg.products_rdr,
    )?;
    let mut events_map: HashMap<Event, u64> = HashMap::default();
    for event in all::<Event>() {
        let md_event =
            md
                .events
                .get_by_name(cfg.org_id, cfg.project_id, event.to_string().as_str()).unwrap();
        events_map.insert(event, md_event.id);
        md.dictionaries
            .get_key_or_create(1, 1, "event_event", event.to_string().as_str()).unwrap();
    }


    let (rx, tx) = bounded(1);
    let schema = db.schema1("events")?;
    // move init to thread because thread_rng is not movable
    // todo parallelize?
    thread::spawn(move || {
        let mut rng = thread_rng();
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

        info!("generating events...");
        let run_cfg = scenario::Config {
            rng: rng.clone(),
            gen,
            schema: Arc::new(schema),
            events_map,
            products,
            to: cfg.to_date,
            out: rx,
        };

        let mut scenario = Scenario::new(run_cfg);

        let res = scenario.run();
        match res {
            Ok(_) => {}
            Err(err) => println!("generation error: {:?}", err)
        }
    });

    while let Some(event) = tx.recv()? {
        let mut vals = vec![];
        vals.push(NamedValue::new("event_project_id".to_string(), Value::Int64(Some(cfg.project_id as i64))));
        vals.push(NamedValue::new("event_created_at".to_string(), Value::Int64(Some(event.created_at))));
        vals.push(NamedValue::new("event_event".to_string(), Value::Int64(Some(event.event))));
        vals.push(NamedValue::new("event_product_name".to_string(), Value::Int16(event.product_name)));
        vals.push(NamedValue::new("event_product_category".to_string(), Value::Int16(event.product_category)));
        vals.push(NamedValue::new("event_product_subcategory".to_string(), Value::Int16(event.product_subcategory)));
        vals.push(NamedValue::new("event_product_brand".to_string(), Value::Int16(event.product_brand)));
        vals.push(NamedValue::new("event_product_price".to_string(), Value::Decimal(event.product_price)));
        vals.push(NamedValue::new("event_product_discount_price".to_string(), Value::Decimal(event.product_discount_price)));
        vals.push(NamedValue::new("event_spent_total".to_string(), Value::Decimal(event.spent_total)));
        vals.push(NamedValue::new("event_products_bought".to_string(), Value::Int8(event.products_bought)));
        vals.push(NamedValue::new("event_cart_items_number".to_string(), Value::Int8(event.cart_items_number)));
        vals.push(NamedValue::new("event_cart_amount".to_string(), Value::Decimal(event.cart_amount)));
        vals.push(NamedValue::new("event_revenue".to_string(), Value::Decimal(event.revenue)));
        vals.push(NamedValue::new("user_country".to_string(), Value::Int16(event.country)));
        vals.push(NamedValue::new("user_city".to_string(), Value::Int16(event.city)));
        vals.push(NamedValue::new("user_device".to_string(), Value::Int16(event.device)));
        vals.push(NamedValue::new("user_device_category".to_string(), Value::Int16(event.device_category)));
        vals.push(NamedValue::new("user_os".to_string(), Value::Int16(event.os)));
        vals.push(NamedValue::new("user_os_version".to_string(), Value::Int16(event.os_version)));
        db.insert("events", vals)?;
    }

    Ok(())
}
