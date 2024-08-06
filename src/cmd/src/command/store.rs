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
use chrono::DateTime;
use chrono::Duration;
use chrono::NaiveDateTime;
use chrono::Utc;
use clap::Parser;
use common::{DATA_PATH_METADATA, DATA_PATH_STORAGE, group_col};
use common::types::COLUMN_EVENT;
use common::types::EVENT_PROPERTY_CITY;
use common::types::EVENT_PROPERTY_COUNTRY;
use common::types::EVENT_PROPERTY_DEVICE_MODEL;
use common::types::EVENT_PROPERTY_OS;
use common::types::EVENT_PROPERTY_OS_FAMILY;
use common::types::EVENT_PROPERTY_OS_VERSION_MAJOR;
use common::types::EVENT_PROPERTY_PAGE_PATH;
use common::types::EVENT_PROPERTY_PAGE_TITLE;
use common::types::EVENT_PROPERTY_PAGE_URL;
use common::types::TABLE_EVENTS;
use common::GROUPS_COUNT;
use common::GROUP_USER_ID;
use crossbeam_channel::bounded;
use dateparser::DateTimeUtc;
use enum_iterator::all;
use events_gen::generator;
use events_gen::generator::Generator;
use events_gen::store::companies::CompanyProvider;
use events_gen::store::events::Event;
use events_gen::store::products::ProductProvider;
use events_gen::store::profiles::ProfileProvider;
use events_gen::store::scenario;
use events_gen::store::scenario::Scenario;
use ingester::error::IngesterError;
use ingester::executor::Executor;
use ingester::transformers::geo;
use ingester::transformers::user_agent;
use ingester::Destination;
use ingester::Identify;
use ingester::Track;
use ingester::Transformer;
use metadata::MetadataProvider;
use platform::projects::init_project;
use query::col_name;
use rand::thread_rng;
use storage::db::OptiDBImpl;
use storage::db::Options;
use storage::NamedValue;
use storage::Value;
use tokio::net::TcpListener;
use tokio::select;
use tokio::signal::unix::SignalKind;
use tracing::debug;
use tracing::info;
use uaparser::UserAgentParser;

use crate::error::Error;
use crate::error::Result;
use crate::{init_config, init_ingester};
use crate::init_metrics;
use crate::init_platform;
use crate::init_session_cleaner;
use crate::init_system;

#[derive(Parser, Clone)]
pub struct Store {
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
    partitions: Option<usize>,
    #[arg(long)]
    pub config: PathBuf,
}

pub struct Config<R> {
    pub project_id: u64,
    pub token: String,
    pub from_date: DateTime<Utc>,
    pub to_date: DateTime<Utc>,
    pub products_rdr: R,
    pub geo_rdr: R,
    pub device_rdr: R,
    pub companies_rdr: R,
    pub new_daily_users: usize,
    pub batch_size: usize,
    pub partitions: usize,
}

pub async fn start(args: &Store, mut cfg: crate::Config) -> Result<()> {
    debug!("db path: {:?}", cfg.data.path);

    if args.generate {
        match fs::remove_dir_all(&cfg.data.path) {
            Ok(_) => {}
            Err(_) => {}
        }
    }
    let rocks = Arc::new(metadata::rocksdb::new(cfg.data.path.join(DATA_PATH_METADATA))?);
    let db = Arc::new(OptiDBImpl::open(
        cfg.data.path.join(DATA_PATH_STORAGE),
        Options {},
    )?);
    let md = Arc::new(MetadataProvider::try_new(rocks, db.clone())?);
    init_config(&md, &mut cfg)?;
    info!("metrics initialization...");
    init_metrics();
    info!("system initialization...");
    init_system(&md, &db, &cfg)?;
    info!("initializing session cleaner...");
    init_session_cleaner(md.clone(), db.clone(), cfg.clone())?;
    info!("initializing backup...");
    init_backup(md.clone(), db.clone(), cfg.clone())?;
    if !cfg.data.ui_path.try_exists()? {
        return Err(Error::FileNotFound(format!(
            "ui path {:?} doesn't exist", cfg.data.ui_path
        )));
    }
    debug!("ui path: {:?}", cfg.data.ui_path);

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
    let proj = crate::init_test_org_structure(&md)?;
    info!("project initialization...");

    init_project(proj.id, &md)?;

    info!("store initialization...");
    debug!("demo data path: {:?}", args.demo_data_path);
    // todo move outside
    if args.generate {
        info!("ingester initialization...");
        // todo make common
        let mut track_destinations = Vec::new();
        let track_local_dst =
            ingester::destinations::local::track::Local::new(db.clone(), md.clone());
        track_destinations.push(Arc::new(track_local_dst) as Arc<dyn Destination<Track>>);
        let track_exec =
            Executor::<Track>::new(vec![], track_destinations.clone(), db.clone(), md.clone());

        let mut identify_destinations = Vec::new();
        let identify_dst =
            ingester::destinations::local::identify::Local::new(db.clone(), md.clone());
        identify_destinations.push(Arc::new(identify_dst) as Arc<dyn Destination<Identify>>);
        let identify_exec =
            Executor::<Identify>::new(vec![], identify_destinations, db.clone(), md.clone());

        info!("generate events...");
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

        let md_clone = md.clone();
        let args_clone = args.clone();
        let db_clone = db.clone();
        thread::spawn(move || {
            let store_cfg = Config {
                project_id: proj.id,
                token: proj.token.clone(),
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
                companies_rdr: File::open(args_clone.demo_data_path.join("companies.csv"))
                    .map_err(|err| Error::Internal(format!("can't open companies.csv: {err}")))
                    .unwrap(),
                new_daily_users: args_clone.new_daily_users,
                batch_size: 4096,
                partitions: args_clone.partitions.unwrap_or_else(num_cpus::get),
            };

            gen(md_clone, db_clone, track_exec, identify_exec, store_cfg).unwrap();
        });
    }

    let router = Router::new();

    info!("initializing platform...");
    let router = init_platform(md.clone(), db.clone(), router, cfg.clone())?;
    info!("initializing ingester...");
    let router = init_ingester(&cfg.data.geo_city_path, &cfg.data.ua_db_path, &md, &db, router)?;

    info!("listening on {}", cfg.server.host);

    let signal = async {
        let mut sig_int =
            tokio::signal::unix::signal(SignalKind::interrupt()).expect("failed to install signal");
        let mut sig_term =
            tokio::signal::unix::signal(SignalKind::terminate()).expect("failed to install signal");
        select! {
            _=sig_int.recv()=>info!("SIGINT received"),
            _=sig_term.recv()=>info!("SIGTERM received"),
        }
    };
    let listener = tokio::net::TcpListener::bind(cfg.server.host).await?;
    Ok(axum::serve(
        listener,
        router.into_make_service_with_connect_info::<SocketAddr>(),
    )
        .await?)
}

pub fn gen<R>(
    md: Arc<MetadataProvider>,
    db: Arc<OptiDBImpl>,
    track: Executor<Track>,
    identify: Executor<Identify>,
    cfg: Config<R>,
) -> Result<()>
where
    R: io::Read,
{
    let mut rng = thread_rng();
    info!("loading products...");
    let products = ProductProvider::try_new_from_csv(&mut rng, cfg.products_rdr)?;
    info!("loading companies...");
    let companies = CompanyProvider::try_new_from_csv(&mut rng, cfg.companies_rdr)?;
    info!("loading profiles...");
    let profiles = ProfileProvider::try_new_from_csv(cfg.geo_rdr, cfg.device_rdr, companies)?;

    // move init to thread because thread_rng is not movable
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
            products,
            to: cfg.to_date,
            track,
            identify,
            user_props_prov: md.group_properties[GROUP_USER_ID].clone(),
            session: md.sessions.clone(),
            project_id: cfg.project_id,
            token: cfg.token,
        };

        let mut scenario = Scenario::new(run_cfg);

        let now = Utc::now();
        let res = scenario.run();
        db.flush(TABLE_EVENTS).unwrap();
        for i in 0..GROUPS_COUNT {
            db.flush(group_col(i).as_str()).unwrap();
        }
        match res {
            Ok(_) => {}
            Err(err) => println!("generation error: {:?}", err),
        }
        let diff = Utc::now() - now;

        info!(
            "generation finished in {}",
            humantime::format_duration(diff.to_std().unwrap())
        );
    });

    Ok(())
}
