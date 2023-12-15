use std::collections::HashMap;
use std::{fs, io, thread};
use std::fs::File;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use crate::error::Result;
use arrow::record_batch::RecordBatch;
use axum::{Router, Server};
use chrono::{DateTime, Duration};
use chrono::Utc;
use clap::Parser;
use crossbeam_channel::bounded;
use datafusion::datasource::TableProvider;
use dateparser::DateTimeUtc;
use enum_iterator::all;
use events_gen::generator;
use events_gen::generator::Generator;
use events_gen::store::events::Event;
use events_gen::store::products::ProductProvider;
use events_gen::store::profiles::ProfileProvider;
use events_gen::store::scenario;
use events_gen::store::scenario::Scenario;
use events_gen::store::schema::create_properties;
use futures::executor::block_on;
use metadata::{MetadataProvider, properties};
use rand::thread_rng;
use scan_dir::ScanDir;
use tokio::select;
use tokio::signal::unix::SignalKind;
use tracing::{debug, info};
use uaparser::UserAgentParser;
use common::rbac::{OrganizationRole, ProjectRole, Role};
use common::types::{COLUMN_PROJECT_ID, DType, USER_PROPERTY_CITY, USER_PROPERTY_CLIENT_FAMILY, USER_PROPERTY_CLIENT_VERSION_MAJOR, USER_PROPERTY_CLIENT_VERSION_MINOR, USER_PROPERTY_CLIENT_VERSION_PATCH, USER_PROPERTY_COUNTRY, USER_PROPERTY_DEVICE_BRAND, USER_PROPERTY_DEVICE_FAMILY, USER_PROPERTY_DEVICE_MODEL, USER_PROPERTY_OS_FAMILY, USER_PROPERTY_OS_VERSION_MAJOR, USER_PROPERTY_OS_VERSION_MINOR, USER_PROPERTY_OS_VERSION_PATCH, USER_PROPERTY_OS_VERSION_PATCH_MINOR};
use ingester::error::IngesterError;
use ingester::{Destination, Identify, Track, Transformer};
use ingester::executor::Executor;
use ingester::transformers::{geo, user_agent};
use metadata::accounts::CreateAccountRequest;
use metadata::organizations::CreateOrganizationRequest;
use metadata::projects::CreateProjectRequest;
use metadata::properties::{CreatePropertyRequest, DictionaryType, Status, Type};
use platform::auth;
use platform::auth::password::make_password_hash;
use query::datasources::local::LocalTable;
use query::ProviderImpl;
use store::db::{OptiDBImpl, Options, TableOptions};
use store::{NamedValue, Value};
use test_util::{create_property, CreatePropertyMainRequest};
use crate::error::Error;
use crate::{init_project, init_system};

#[derive(Parser, Clone)]
pub struct Shop {
    #[arg(long)]
    path: PathBuf,
    #[arg(long, default_value = "0.0.0.0:8080")]
    host: SocketAddr,
    #[arg(long)]
    demo_data_path: PathBuf,
    #[arg(long, default_value = "365 days")]
    duration: Option<String>,
    #[arg(long)]
    to_date: Option<String>,
    #[arg(long, default_value = "10")]
    new_daily_users: usize,
    #[arg(long)]
    ui_path: Option<PathBuf>,
    #[arg(long)]
    partitions: Option<usize>,
    #[arg(long)]
    ua_db_path: PathBuf,
    #[arg(long)]
    geo_city_path: PathBuf,

}

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

fn init_platform(md: &Arc<MetadataProvider>, db: &Arc<OptiDBImpl>, router: Router) -> Result<Router> {
    let data_provider: Arc<dyn TableProvider> = Arc::new(LocalTable::try_new(db.clone(), "events".to_string())?);
    let query_provider = Arc::new(ProviderImpl::try_new_from_provider(
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

    info!("attaching platform routes...");
    Ok(platform::http::attach_routes(router, &md, &platform_provider, auth_cfg, None))
}

fn init_ingester(args: &Shop, md: &Arc<MetadataProvider>, db: &Arc<OptiDBImpl>, router: Router) -> Result<Router> {
    let mut track_transformers = Vec::new();
    let ua_parser = UserAgentParser::from_file(File::open(args.ua_db_path.clone())?)
        .map_err(|e| Error::Internal(e.to_string()))?;
    let ua = user_agent::track::UserAgent::try_new(md.user_properties.clone(), ua_parser)?;
    track_transformers.push(Arc::new(ua) as Arc<dyn Transformer<Track>>);

    // todo make common
    let city_rdr = maxminddb::Reader::open_readfile(args.geo_city_path.clone())?;
    let geo = geo::track::Geo::try_new(md.user_properties.clone(), city_rdr)?;
    track_transformers.push(Arc::new(geo) as Arc<dyn Transformer<Track>>);

    let mut track_destinations = Vec::new();
    let track_local_dst = ingester::destinations::local::track::Local::new(
        db.clone(),
        md.dictionaries.clone(),
        md.event_properties.clone(),
        md.user_properties.clone(),
    );
    track_destinations.push(Arc::new(track_local_dst) as Arc<dyn Destination<Track>>);
    let track_exec = Executor::<Track>::new(
        track_transformers.clone(),
        track_destinations.clone(),
        db.clone(),
        md.event_properties.clone(),
        md.user_properties.clone(),
        md.events.clone(),
        md.projects.clone(),
        md.dictionaries.clone(),
    );

    let mut identify_transformers = Vec::new();
    info!("initializing ua parser...");
    let ua_parser = UserAgentParser::from_file(File::open(args.ua_db_path.clone())?)
        .map_err(|e| IngesterError::Internal(e.to_string()))?;
    let ua = user_agent::identify::UserAgent::try_new(md.user_properties.clone(), ua_parser)?;
    identify_transformers.push(Arc::new(ua) as Arc<dyn Transformer<Identify>>);

    info!("initializing geo...");
    let city_rdr = maxminddb::Reader::open_readfile(args.geo_city_path.clone())?;
    let geo = geo::identify::Geo::try_new(md.user_properties.clone(), city_rdr)?;
    identify_transformers.push(Arc::new(geo) as Arc<dyn Transformer<Identify>>);
    let mut identify_destinations = Vec::new();
    let identify_debug_dst = ingester::destinations::debug::identify::Debug::new();
    identify_destinations.push(Arc::new(identify_debug_dst) as Arc<dyn Destination<Identify>>);
    let identify_exec = Executor::<Identify>::new(
        identify_transformers,
        identify_destinations,
        db.clone(),
        md.event_properties.clone(),
        md.user_properties.clone(),
        md.events.clone(),
        md.projects.clone(),
        md.dictionaries.clone(),
    );

    info!("attaching ingester routes...");
    Ok(ingester::sources::http::attach_routes(router, track_exec, identify_exec))
}

pub async fn start(args: &Shop, proj_id: u64) -> Result<()> {
    debug!("db path: {:?}", args.path);

    fs::remove_dir_all(&args.path).unwrap();
    let rocks = Arc::new(metadata::rocksdb::new(args.path.join("md"))?);
    let db = Arc::new(OptiDBImpl::open(args.path.join("store"), Options {})?);
    let md = Arc::new(MetadataProvider::try_new(rocks, db.clone())?);
    info!("system initialization...");
    init_system(&md, &db)?;
    if let Some(ui_path) = &args.ui_path {
        if !ui_path.try_exists()? {
            return Err(
                Error::FileNotFound(format!("ui path {ui_path:?} doesn't exist")).into(),
            );
        }
        debug!("ui path: {:?}", ui_path);
    }

    if !args.demo_data_path.try_exists()? {
        return Err(Error::FileNotFound(format!(
            "demo data path {:?} doesn't exist",
            args.demo_data_path
        ))
            .into());
    }

    let to_date = match &args.to_date {
        None => Utc::now(),
        Some(dt) => dt.parse::<DateTimeUtc>()?.0.with_timezone(&Utc),
    };

    let duration = Duration::from_std(parse_duration::parse(
        args.duration.clone().unwrap().as_str(),
    )?)?;
    let from_date = to_date - duration;

    info!("creating org structure and admin account...");
    let (org_id, proj_id) = crate::init_test_org_structure(&md)?;
    info!("project initialization...");
    init_project(org_id, proj_id, &md)?;

    info!("store initialization...");
    debug!("demo data path: {:?}", args.demo_data_path);
    debug!("from date {}", from_date);
    let date_diff = to_date - from_date;
    debug!("to date {}", to_date);
    debug!(
        "time range: {}",
        humantime::format_duration(date_diff.to_std()?)
    );
    debug!("new daily users: {}", args.new_daily_users);
    let total_users = args.new_daily_users as i64 * date_diff.num_days();
    info!("expecting total unique users: {total_users}");
    info!("starting sample data generation...");

    let store_cfg = Config {
        org_id: 1,
        project_id: proj_id,
        from_date,
        to_date,
        products_rdr: File::open(args.demo_data_path.join("products.csv"))
            .map_err(|err| Error::Internal(format!("can't open products.csv: {err}")))?,
        geo_rdr: File::open(args.demo_data_path.join("geo.csv"))
            .map_err(|err| Error::Internal(format!("can't open geo.csv: {err}")))?,
        device_rdr: File::open(args.demo_data_path.join("device.csv"))
            .map_err(|err| Error::Internal(format!("can't open device.csv: {err}")))?,
        new_daily_users: args.new_daily_users,
        batch_size: 4096,
        partitions: args.partitions.unwrap_or_else(num_cpus::get),
    };

    gen(&md, &db, store_cfg)?;
    db.flush()?;

    info!("successfully generated!");

    let mut router = Router::new();
    info!("initializing platform...");
    let router = init_platform(&md, &db, router)?;
    info!("initializing ingester...");
    let router = init_ingester(args, &md, &db, router)?;

    let server = Server::bind(&args.host).serve(router.into_make_service_with_connect_info::<SocketAddr>());
    info!("start listening on {}", args.host);
    let graceful = server.with_graceful_shutdown(async {
        let mut sig_int = tokio::signal::unix::signal(SignalKind::interrupt())
            .expect("failed to install signal");
        let mut sig_term = tokio::signal::unix::signal(SignalKind::terminate())
            .expect("failed to install signal");
        select! {
                _=sig_int.recv()=>info!("SIGINT received"),
                _=sig_term.recv()=>info!("SIGTERM received"),
            }
    });

    Ok(graceful.await?)
}

pub fn gen<R>(
    md: &Arc<MetadataProvider>,
    db: &Arc<OptiDBImpl>,
    cfg: Config<R>,
) -> Result<()>
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
        max_log_length_bytes: 1024 * 1024 * 10,
        merge_array_page_size: 10000,
        merge_data_page_size_limit_bytes: Some(1024 * 1024),
        merge_index_cols: 2,
        merge_max_l1_part_size_bytes: 1024 * 1024,
        merge_part_size_multiplier: 10,
        merge_row_group_values_limit: 1000,
        merge_chunk_size: 1024 * 8 * 8,
    };
    db.create_table("events", topts)?;
    info!("creating properties...");
    create_properties(cfg.org_id, cfg.project_id, md, db)?;

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

    let mut idx = 0;
    while let Some(event) = tx.recv()? {
        let mut vals = vec![];
        vals.push(NamedValue::new("event_project_id".to_string(), Value::Int64(Some(cfg.project_id as i64))));
        vals.push(NamedValue::new("event_user_id".to_string(), Value::Int64(Some(event.user_id))));
        vals.push(NamedValue::new("event_created_at".to_string(), Value::Int64(Some(event.created_at))));
        vals.push(NamedValue::new("event_event_id".to_string(), Value::Int64(Some(idx))));
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
        idx += 1;
    }

    Ok(())
}
