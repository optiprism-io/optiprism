use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;

use axum::Router;
use axum::Server;
use chrono::DateTime;
use chrono::Duration;
use chrono::NaiveDateTime;
use chrono::Utc;
use clap::Parser;
use common::types::EVENT_PROPERTY_PAGE_PATH;
use common::types::EVENT_PROPERTY_PAGE_TITLE;
use common::types::EVENT_PROPERTY_PAGE_URL;
use common::types::USER_PROPERTY_CITY;
use common::types::USER_PROPERTY_COUNTRY;
use common::types::USER_PROPERTY_DEVICE_MODEL;
use common::types::USER_PROPERTY_OS;
use common::types::USER_PROPERTY_OS_FAMILY;
use common::types::USER_PROPERTY_OS_VERSION_MAJOR;
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
use events_gen::store::scenario::EventRecord;
use events_gen::store::scenario::Scenario;
use events_gen::store::schema::create_properties;
use ingester::error::IngesterError;
use ingester::executor::Executor;
use ingester::transformers::geo;
use ingester::transformers::user_agent;
use ingester::Destination;
use ingester::Identify;
use ingester::Track;
use ingester::Transformer;
use metadata::MetadataProvider;
use platform::auth;
use query::datasources::local::LocalTable;
use query::QueryProvider;
use rand::thread_rng;
use store::db::OptiDBImpl;
use store::db::Options;
use store::NamedValue;
use store::Value;
use tokio::select;
use tokio::signal::unix::SignalKind;
use tracing::debug;
use tracing::info;
use uaparser::UserAgentParser;

use crate::error::Error;
use crate::error::Result;
use crate::init_metrics;
use crate::init_project;
use crate::init_system;

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
    #[arg(long, default_value = "10 days")]
    future_duration: Option<String>,
    #[arg(long, default_value = "10")]
    new_daily_users: usize,
    #[arg(long, default_value = "false")]
    generate: bool,
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

fn init_platform(md: Arc<MetadataProvider>, db: Arc<OptiDBImpl>, router: Router) -> Result<Router> {
    let data_provider: Arc<dyn TableProvider> =
        Arc::new(LocalTable::try_new(db.clone(), "events".to_string())?);
    let query_provider = Arc::new(QueryProvider::try_new_from_provider(
        md.clone(),
        db.clone(),
        data_provider,
    )?);

    let auth_cfg = auth::provider::Config {
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
    Ok(platform::http::attach_routes(
        router,
        &md,
        &platform_provider,
        auth_cfg,
        None,
    ))
}

fn init_ingester(
    args: &Shop,
    md: &Arc<MetadataProvider>,
    db: &Arc<OptiDBImpl>,
    router: Router,
) -> Result<Router> {
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
    let track_local_dst = ingester::destinations::local::track::Local::new(db.clone(), md.clone());
    track_destinations.push(Arc::new(track_local_dst) as Arc<dyn Destination<Track>>);
    let track_exec = Executor::<Track>::new(
        track_transformers.clone(),
        track_destinations.clone(),
        db.clone(),
        md.clone(),
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
        md.clone(),
    );

    info!("attaching ingester routes...");
    Ok(ingester::sources::http::attach_routes(
        router,
        track_exec,
        identify_exec,
    ))
}

pub async fn start(args: &Shop, _proj_id: u64) -> Result<()> {
    debug!("db path: {:?}", args.path);

    if args.generate {
        fs::remove_dir_all(&args.path).unwrap();
    }
    let rocks = Arc::new(metadata::rocksdb::new(args.path.join("md"))?);
    let db = Arc::new(OptiDBImpl::open(args.path.join("store"), Options {})?);
    let md = Arc::new(MetadataProvider::try_new(rocks, db.clone())?);
    info!("metrics initialization...");
    init_metrics();
    info!("system initialization...");
    init_system(&md, &db, args.partitions.unwrap_or_else(num_cpus::get))?;

    if let Some(ui_path) = &args.ui_path {
        if !ui_path.try_exists()? {
            return Err(Error::FileNotFound(format!(
                "ui path {ui_path:?} doesn't exist"
            )));
        }
        debug!("ui path: {:?}", ui_path);
    }

    if !args.demo_data_path.try_exists()? {
        return Err(Error::FileNotFound(format!(
            "demo data path {:?} doesn't exist",
            args.demo_data_path
        )));
    }

    let to_date = match &args.to_date {
        None => Utc::now(),
        Some(dt) => dt.parse::<DateTimeUtc>()?.0.with_timezone(&Utc),
    };

    let duration = Duration::from_std(parse_duration::parse(
        args.duration.clone().unwrap().as_str(),
    )?)?;
    let future_duration = Duration::from_std(parse_duration::parse(
        args.future_duration.clone().unwrap().as_str(),
    )?)?;
    let from_date = to_date - duration;

    info!("creating org structure and admin account...");
    let (org_id, proj_id) = crate::init_test_org_structure(&md)?;
    info!("project initialization...");

    init_project(org_id, proj_id, &md)?;

    info!("store initialization...");
    debug!("demo data path: {:?}", args.demo_data_path);
    // todo move outside
    if args.generate {
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

        let _db_clone = db.clone();
        let md_clone = md.clone();
        let db_clone = db.clone();
        let args_clone = args.clone();
        thread::spawn(move || {
            let store_cfg = Config {
                org_id: 1,
                project_id: proj_id,
                from_date,
                to_date,
                products_rdr: File::open(args_clone.demo_data_path.join("products.csv"))
                    .map_err(|err| Error::Internal(format!("can't open products.csv: {err}")))
                    .unwrap(),
                geo_rdr: File::open(args_clone.demo_data_path.join("geo.csv"))
                    .map_err(|err| Error::Internal(format!("can't open geo.csv: {err}")))
                    .unwrap(),
                device_rdr: File::open(args_clone.demo_data_path.join("device.csv"))
                    .map_err(|err| Error::Internal(format!("can't open device.csv: {err}")))
                    .unwrap(),
                new_daily_users: args_clone.new_daily_users,
                batch_size: 4096,
                partitions: args_clone.partitions.unwrap_or_else(num_cpus::get),
            };

            gen(md_clone, db_clone.clone(), store_cfg).unwrap();
            db_clone.flush().unwrap();
        });

        info!("successfully generated!");

        info!("starting future data generation...");
        let store_cfg = Config {
            org_id: 1,
            project_id: proj_id,
            from_date: to_date,
            to_date: to_date + future_duration,
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
        future_gen(md.clone(), db.clone(), store_cfg)?;
    }

    let router = Router::new();
    info!("initializing platform...");
    let router = init_platform(md.clone(), db.clone(), router)?;
    info!("initializing ingester...");
    let router = init_ingester(args, &md, &db, router)?;

    let server =
        Server::bind(&args.host).serve(router.into_make_service_with_connect_info::<SocketAddr>());
    info!("start listening on {}", args.host);
    let graceful = server.with_graceful_shutdown(async {
        let mut sig_int =
            tokio::signal::unix::signal(SignalKind::interrupt()).expect("failed to install signal");
        let mut sig_term =
            tokio::signal::unix::signal(SignalKind::terminate()).expect("failed to install signal");
        select! {
            _=sig_int.recv()=>info!("SIGINT received"),
            _=sig_term.recv()=>info!("SIGTERM received"),
        }
    });

    Ok(graceful.await?)
}

pub fn gen<R>(md: Arc<MetadataProvider>, db: Arc<OptiDBImpl>, cfg: Config<R>) -> Result<()>
where R: io::Read {
    let mut rng = thread_rng();
    info!("creating entities...");

    info!("creating properties...");
    create_properties(cfg.org_id, cfg.project_id, &md, &db)?;

    info!("loading profiles...");
    let profiles = ProfileProvider::try_new_from_csv(
        cfg.org_id,
        cfg.project_id,
        &md.dictionaries,
        &md.user_properties,
        cfg.geo_rdr,
        cfg.device_rdr,
    )?;
    info!("loading products...");
    let products = ProductProvider::try_new_from_csv(
        cfg.org_id,
        cfg.project_id,
        &mut rng,
        md.dictionaries.clone(),
        md.event_properties.clone(),
        cfg.products_rdr,
    )?;
    let mut events_map: HashMap<Event, u64> = HashMap::default();
    for event in all::<Event>() {
        let md_event = md
            .events
            .get_by_name(cfg.org_id, cfg.project_id, event.to_string().as_str())
            .unwrap();
        events_map.insert(event, md_event.id);
        md.dictionaries
            .get_key_or_create(1, 1, "event_event", event.to_string().as_str())
            .unwrap();
    }

    let (rx, tx) = bounded(1);
    // move init to thread because thread_rng is not movable
    // todo parallelize?
    thread::spawn(move || {
        let rng = thread_rng();
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
            events_map,
            products,
            to: cfg.to_date,
            out: rx,
        };

        let mut scenario = Scenario::new(run_cfg);

        let res = scenario.run();
        match res {
            Ok(_) => {}
            Err(err) => println!("generation error: {:?}", err),
        }
    });

    let mut idx = 0;
    while let Some(event) = tx.recv()? {
        write_event(cfg.org_id, cfg.project_id, &db, &md, event, idx)?;
        idx += 1;
        if idx % 1000000 == 0 {
            println!("{idx}");
        }
    }

    Ok(())
}

pub fn future_gen<R>(md: Arc<MetadataProvider>, db: Arc<OptiDBImpl>, cfg: Config<R>) -> Result<()>
where R: io::Read {
    let mut rng = thread_rng();
    let profiles = ProfileProvider::try_new_from_csv(
        cfg.org_id,
        cfg.project_id,
        &md.dictionaries,
        &md.user_properties,
        cfg.geo_rdr,
        cfg.device_rdr,
    )?;
    let products = ProductProvider::try_new_from_csv(
        cfg.org_id,
        cfg.project_id,
        &mut rng,
        md.dictionaries.clone(),
        md.event_properties.clone(),
        cfg.products_rdr,
    )?;
    let mut events_map: HashMap<Event, u64> = HashMap::default();
    for event in all::<Event>() {
        let md_event = md
            .events
            .get_by_name(cfg.org_id, cfg.project_id, event.to_string().as_str())
            .unwrap();
        events_map.insert(event, md_event.id);
        md.dictionaries
            .get_key_or_create(1, 1, "event_event", event.to_string().as_str())
            .unwrap();
    }

    let (rx, tx) = bounded(1);

    // todo parallelize?
    thread::spawn(move || {
        let rng = thread_rng();
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

        let run_cfg = scenario::Config {
            rng: rng.clone(),
            gen,
            events_map,
            products,
            to: cfg.to_date,
            out: rx,
        };

        let mut scenario = Scenario::new(run_cfg);

        let res = scenario.run();
        match res {
            Ok(_) => {}
            Err(err) => println!("generation error: {:?}", err),
        }
    });

    let mut out = BTreeMap::new();
    while let Some(event) = tx.recv()? {
        out.insert(event.created_at, event);
    }
    thread::spawn(move || {
        loop {
            match out.pop_first() {
                None => break,
                Some((ts, event)) => {
                    let cur = Utc::now().timestamp();
                    let ts = ts / 1000000000;
                    let _d = NaiveDateTime::from_timestamp_opt(ts, 0).unwrap();
                    if ts > cur {
                        thread::sleep(std::time::Duration::from_secs((ts - cur) as u64));
                    }
                    write_event(cfg.org_id, cfg.project_id, &db, &md, event, 0).unwrap();
                }
            }
        }
    });
    Ok(())
}

fn write_event(
    org_id: u64,
    proj_id: u64,
    db: &Arc<OptiDBImpl>,
    md: &Arc<MetadataProvider>,
    event: EventRecord,
    idx: i64,
) -> Result<()> {
    let vals = vec![
        NamedValue::new("project_id".to_string(), Value::Int64(Some(proj_id as i64))),
        NamedValue::new("user_id".to_string(), Value::Int64(Some(event.user_id))),
        NamedValue::new(
            "created_at".to_string(),
            Value::Timestamp(Some(event.created_at)),
        ),
        NamedValue::new("event_id".to_string(), Value::Int64(Some(idx))),
        NamedValue::new("event".to_string(), Value::Int64(Some(event.event))),
        NamedValue::new(
            md.event_properties
                .get_by_name(org_id, proj_id, EVENT_PROPERTY_PAGE_PATH)
                .unwrap()
                .column_name(),
            Value::String(Some(event.page_path)),
        ),
        NamedValue::new(
            md.event_properties
                .get_by_name(org_id, proj_id, EVENT_PROPERTY_PAGE_TITLE)
                .unwrap()
                .column_name(),
            Value::String(Some(event.page_title)),
        ),
        NamedValue::new(
            md.event_properties
                .get_by_name(org_id, proj_id, EVENT_PROPERTY_PAGE_URL)
                .unwrap()
                .column_name(),
            Value::String(Some(event.page_url)),
        ),
        NamedValue::new(
            md.event_properties
                .get_by_name(org_id, proj_id, "Product Name")
                .unwrap()
                .column_name(),
            Value::Int16(event.product_name),
        ),
        NamedValue::new(
            md.event_properties
                .get_by_name(org_id, proj_id, "Product Category")
                .unwrap()
                .column_name(),
            Value::Int16(event.product_category),
        ),
        NamedValue::new(
            md.event_properties
                .get_by_name(org_id, proj_id, "Product Subcategory")
                .unwrap()
                .column_name(),
            Value::Int16(event.product_subcategory),
        ),
        NamedValue::new(
            md.event_properties
                .get_by_name(org_id, proj_id, "Product Brand")
                .unwrap()
                .column_name(),
            Value::Int16(event.product_brand),
        ),
        NamedValue::new(
            md.event_properties
                .get_by_name(org_id, proj_id, "Product Price")
                .unwrap()
                .column_name(),
            Value::Decimal(event.product_price),
        ),
        NamedValue::new(
            md.event_properties
                .get_by_name(org_id, proj_id, "Product Discount Price")
                .unwrap()
                .column_name(),
            Value::Decimal(event.product_discount_price),
        ),
        NamedValue::new(
            md.event_properties
                .get_by_name(org_id, proj_id, "Revenue")
                .unwrap()
                .column_name(),
            Value::Decimal(event.revenue),
        ),
        NamedValue::new(
            md.user_properties
                .get_by_name(org_id, proj_id, "Spent Total")
                .unwrap()
                .column_name(),
            Value::Decimal(event.spent_total),
        ),
        NamedValue::new(
            md.user_properties
                .get_by_name(org_id, proj_id, "Products Bought")
                .unwrap()
                .column_name(),
            Value::Int8(event.products_bought),
        ),
        NamedValue::new(
            md.user_properties
                .get_by_name(org_id, proj_id, "Cart Items Number")
                .unwrap()
                .column_name(),
            Value::Int8(event.cart_items_number),
        ),
        NamedValue::new(
            md.user_properties
                .get_by_name(org_id, proj_id, "Cart Amount")
                .unwrap()
                .column_name(),
            Value::Decimal(event.cart_amount),
        ),
        NamedValue::new(
            md.user_properties
                .get_by_name(org_id, proj_id, USER_PROPERTY_COUNTRY)
                .unwrap()
                .column_name(),
            Value::Int64(event.country.map(|v| v as i64)),
        ),
        NamedValue::new(
            md.user_properties
                .get_by_name(org_id, proj_id, USER_PROPERTY_CITY)
                .unwrap()
                .column_name(),
            Value::Int64(event.city.map(|v| v as i64)),
        ),
        NamedValue::new(
            md.user_properties
                .get_by_name(org_id, proj_id, USER_PROPERTY_DEVICE_MODEL)
                .unwrap()
                .column_name(),
            Value::Int64(event.device.map(|v| v as i64)),
        ),
        NamedValue::new(
            md.user_properties
                .get_by_name(org_id, proj_id, USER_PROPERTY_OS_FAMILY)
                .unwrap()
                .column_name(),
            Value::Int64(event.device_category.map(|v| v as i64)),
        ),
        NamedValue::new(
            md.user_properties
                .get_by_name(org_id, proj_id, USER_PROPERTY_OS)
                .unwrap()
                .column_name(),
            Value::Int64(event.os.map(|v| v as i64)),
        ),
        NamedValue::new(
            md.user_properties
                .get_by_name(org_id, proj_id, USER_PROPERTY_OS_VERSION_MAJOR)
                .unwrap()
                .column_name(),
            Value::Int64(event.os_version.map(|v| v as i64)),
        ),
    ];
    db.insert("events", vals)?;

    Ok(())
}
