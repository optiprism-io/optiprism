#![feature(async_closure)]

extern crate core;

use std::fs;
use std::fs::File;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::Duration as StdDuration;

use ::storage::db::OptiDBImpl;
use ::storage::error::StoreError;
use ::storage::table::Options as TableOptions;
use ::storage::NamedValue;
use ::storage::Value;
use axum::Router;
use chrono::Utc;
use common::config::Config;
use common::group_col;
use common::rbac::OrganizationRole;
use common::rbac::ProjectRole;
use common::rbac::Role;
use common::types::COLUMN_CREATED_AT;
use common::types::COLUMN_EVENT;
use common::types::COLUMN_EVENT_ID;
use common::types::COLUMN_PROJECT_ID;
use common::types::EVENT_PROPERTY_SESSION_LENGTH;
use common::types::EVENT_SESSION_END;
use common::types::METRIC_HTTP_REQUESTS_TOTAL;
use common::types::METRIC_HTTP_REQUEST_TIME_SECONDS;
use common::types::METRIC_INGESTER_IDENTIFIED_TOTAL;
use common::types::METRIC_INGESTER_IDENTIFY_TIME_SECONDS;
use common::types::METRIC_INGESTER_TRACKED_TOTAL;
use common::types::METRIC_INGESTER_TRACK_TIME_SECONDS;
use common::types::METRIC_QUERY_EXECUTION_TIME_SECONDS;
use common::types::METRIC_QUERY_QUERIES_TOTAL;
use common::types::METRIC_STORE_COMPACTIONS_TOTAL;
use common::types::METRIC_STORE_COMPACTION_TIME_SECONDS;
use common::types::METRIC_STORE_FLUSHES_TOTAL;
use common::types::METRIC_STORE_FLUSH_TIME_SECONDS;
use common::types::METRIC_STORE_INSERTS_TOTAL;
use common::types::METRIC_STORE_INSERT_TIME_SECONDS;
use common::types::METRIC_STORE_LEVEL_COMPACTION_TIME_SECONDS;
use common::types::METRIC_STORE_MEMTABLE_ROWS;
use common::types::METRIC_STORE_MERGES_TOTAL;
use common::types::METRIC_STORE_MERGE_TIME_SECONDS;
use common::types::METRIC_STORE_PARTS;
use common::types::METRIC_STORE_PARTS_SIZE_BYTES;
use common::types::METRIC_STORE_PARTS_VALUES;
use common::types::METRIC_STORE_PART_SIZE_BYTES;
use common::types::METRIC_STORE_PART_VALUES;
use common::types::METRIC_STORE_RECOVERY_TIME_SECONDS;
use common::types::METRIC_STORE_SCANS_TOTAL;
use common::types::METRIC_STORE_SCAN_MEMTABLE_SECONDS;
use common::types::METRIC_STORE_SCAN_PARTS;
use common::types::METRIC_STORE_SCAN_TIME_SECONDS;
use common::types::METRIC_STORE_TABLE_FIELDS;
use common::types::TABLE_EVENTS;
use common::ADMIN_ID;
use common::DATA_PATH_BACKUPS;
use common::DATA_PATH_BACKUP_TMP;
use common::DATA_PATH_METADATA;
use common::DATA_PATH_RECOVERS;
use common::DATA_PATH_STORAGE;
use common::GROUPS_COUNT;
use common::GROUP_USER_ID;
use ingester::executor::Executor;
use ingester::transformers::geo;
use ingester::transformers::user_agent;
use ingester::Destination;
use ingester::Identify;
use ingester::Track;
use ingester::Transformer;
use metadata::accounts::CreateAccountRequest;
use metadata::error::MetadataError;
use metadata::organizations::CreateOrganizationRequest;
use metadata::projects::CreateProjectRequest;
use metadata::projects::Project;
use metadata::MetadataProvider;
use metrics::describe_counter;
use metrics::describe_gauge;
use metrics::describe_histogram;
use metrics::Unit;
use metrics_exporter_prometheus::PrometheusBuilder;
use metrics_util::MetricKindMask;
use platform::auth::password::make_password_hash;
use platform::PlatformProvider;
use query::event_records::EventRecordsProvider;
use query::event_segmentation::EventSegmentationProvider;
use query::funnel::FunnelProvider;
use query::group_records::GroupRecordsProvider;
use query::properties::PropertiesProvider;
use rand::distributions::Alphanumeric;
use rand::distributions::DistString;
use rand::rngs::StdRng;
use rand::thread_rng;
use rand::Rng;
use rand::SeedableRng;
use tracing::info;
use uaparser::UserAgentParser;

use crate::error::Error;
use crate::error::Result;
pub mod backup;
pub mod command;
pub mod config;
pub mod error;

pub fn init_metrics() {
    PrometheusBuilder::new()
        .idle_timeout(
            MetricKindMask::COUNTER | MetricKindMask::HISTOGRAM | MetricKindMask::GAUGE,
            Some(StdDuration::from_secs(10)),
        )
        .with_http_listener(SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 9102))
        .install()
        .expect("failed to install Prometheus recorder");

    describe_counter!(METRIC_STORE_INSERTS_TOTAL, "number of inserts processed");
    describe_histogram!(
        METRIC_STORE_INSERT_TIME_SECONDS,
        Unit::Seconds,
        "insert time"
    );
    describe_counter!(METRIC_STORE_SCANS_TOTAL, "number of scans processed");
    describe_histogram!(METRIC_STORE_SCAN_TIME_SECONDS, Unit::Seconds, "scan time");
    describe_gauge!(METRIC_STORE_SCAN_PARTS, "number of scans parts");
    describe_counter!(METRIC_STORE_MERGES_TOTAL, "number of merges during scan");
    describe_histogram!(METRIC_STORE_MERGE_TIME_SECONDS, Unit::Seconds, "merge time");
    describe_gauge!(METRIC_STORE_TABLE_FIELDS, "number of table fields");
    describe_gauge!(METRIC_STORE_PARTS_SIZE_BYTES, "parts size in bytes");
    describe_gauge!(METRIC_STORE_PART_SIZE_BYTES, "part size in bytes");
    describe_gauge!(METRIC_STORE_PARTS, "parts");
    describe_gauge!(METRIC_STORE_PART_VALUES, "part values");
    describe_gauge!(METRIC_STORE_PARTS_VALUES, "parts values");
    describe_gauge!(METRIC_STORE_MEMTABLE_ROWS, "number of memtable rows");
    describe_histogram!(
        METRIC_STORE_SCAN_MEMTABLE_SECONDS,
        Unit::Seconds,
        "scan memtable time"
    );
    describe_counter!(METRIC_STORE_COMPACTIONS_TOTAL, "number of compactions");
    describe_histogram!(
        METRIC_STORE_LEVEL_COMPACTION_TIME_SECONDS,
        Unit::Seconds,
        "level compaction time"
    );
    describe_histogram!(
        METRIC_STORE_COMPACTION_TIME_SECONDS,
        Unit::Seconds,
        "compaction time"
    );
    describe_histogram!(
        METRIC_STORE_RECOVERY_TIME_SECONDS,
        Unit::Seconds,
        "recovery time"
    );

    describe_histogram!(
        METRIC_STORE_FLUSH_TIME_SECONDS,
        Unit::Seconds,
        "recovery time"
    );
    describe_counter!(METRIC_STORE_FLUSHES_TOTAL, "number of flushes");

    describe_counter!(METRIC_INGESTER_TRACKED_TOTAL, "total number of tracks");
    describe_histogram!(
        METRIC_INGESTER_TRACK_TIME_SECONDS,
        Unit::Seconds,
        "ingester track time"
    );
    describe_counter!(
        METRIC_INGESTER_IDENTIFIED_TOTAL,
        "total number of identifies"
    );
    describe_histogram!(
        METRIC_INGESTER_IDENTIFY_TIME_SECONDS,
        Unit::Seconds,
        "ingester identify time"
    );

    describe_counter!(METRIC_QUERY_QUERIES_TOTAL, "total number of queries");
    describe_histogram!(
        METRIC_QUERY_EXECUTION_TIME_SECONDS,
        Unit::Seconds,
        "query execution time"
    );

    describe_histogram!(
        METRIC_HTTP_REQUEST_TIME_SECONDS,
        Unit::Seconds,
        "http execution time"
    );

    describe_counter!(METRIC_HTTP_REQUESTS_TOTAL, "total number of http requests");
    describe_counter!(METRIC_QUERY_QUERIES_TOTAL, "total number of queries");
    describe_histogram!(
        METRIC_QUERY_EXECUTION_TIME_SECONDS,
        Unit::Seconds,
        "query execution time"
    );
}

pub fn init_fs(cfg: &Config) -> Result<()> {
    fs::create_dir_all(cfg.data.path.join(DATA_PATH_STORAGE))?;
    fs::create_dir_all(cfg.data.path.join(DATA_PATH_METADATA))?;
    fs::create_dir_all(cfg.data.path.join(DATA_PATH_BACKUP_TMP))?;
    fs::create_dir_all(cfg.data.path.join(DATA_PATH_BACKUPS))?;
    fs::create_dir_all(cfg.data.path.join(DATA_PATH_RECOVERS))?;

    Ok(())
}

pub fn clenaup_fs(cfg: &Config) -> Result<()> {
    fs::remove_dir_all(cfg.data.path.join(DATA_PATH_STORAGE))?;
    fs::remove_dir_all(cfg.data.path.join(DATA_PATH_METADATA))?;
    fs::remove_dir_all(cfg.data.path.join(DATA_PATH_BACKUP_TMP))?;
    fs::remove_dir_all(cfg.data.path.join(DATA_PATH_BACKUPS))?;
    fs::remove_dir_all(cfg.data.path.join(DATA_PATH_RECOVERS))?;

    Ok(())
}
pub async fn init_system(
    md: &Arc<MetadataProvider>,
    db: &Arc<OptiDBImpl>,
    cfg: &Config,
) -> error::Result<()> {
    let events_table = TableOptions {
        levels: cfg.events_table.levels,
        merge_array_size: cfg.events_table.merge_array_size,
        index_cols: 2,
        l1_max_size_bytes: cfg.events_table.l1_max_size_bytes,
        level_size_multiplier: cfg.events_table.level_size_multiplier,
        l0_max_parts: cfg.events_table.l0_max_parts,
        max_log_length_bytes: cfg.events_table.max_log_length_bytes,
        merge_array_page_size: cfg.events_table.merge_array_page_size,
        merge_data_page_size_limit_bytes: Some(cfg.events_table.merge_data_page_size_limit_bytes),
        merge_max_l1_part_size_bytes: cfg.events_table.merge_max_l1_part_size_bytes,
        merge_part_size_multiplier: cfg.events_table.merge_part_size_multiplier,
        merge_row_group_values_limit: cfg.events_table.merge_row_group_values_limit,
        merge_chunk_size: cfg.events_table.merge_chunk_size,
        merge_max_page_size: cfg.events_table.merge_max_page_size,
        is_replacing: false,
    };
    match db.create_table(TABLE_EVENTS.to_string(), events_table) {
        Ok(_) => {}
        Err(err) => match err {
            StoreError::AlreadyExists(_) => {}
            other => return Err(other.into()),
        },
    }

    for g in 0..GROUPS_COUNT {
        let tbl = TableOptions {
            levels: cfg.group_table.levels,
            merge_array_size: cfg.group_table.merge_array_size,
            index_cols: 2,
            l1_max_size_bytes: cfg.group_table.l1_max_size_bytes,
            level_size_multiplier: cfg.group_table.level_size_multiplier,
            l0_max_parts: cfg.group_table.l0_max_parts,
            max_log_length_bytes: cfg.group_table.max_log_length_bytes,
            merge_array_page_size: cfg.group_table.merge_array_page_size,
            merge_data_page_size_limit_bytes: Some(
                cfg.group_table.merge_data_page_size_limit_bytes,
            ),
            merge_max_l1_part_size_bytes: cfg.group_table.merge_max_l1_part_size_bytes,
            merge_part_size_multiplier: cfg.group_table.merge_part_size_multiplier,
            merge_row_group_values_limit: cfg.group_table.merge_row_group_values_limit,
            merge_chunk_size: cfg.group_table.merge_chunk_size,
            merge_max_page_size: cfg.group_table.merge_max_page_size,
            is_replacing: true,
        };

        let name = group_col(g);
        match db.create_table(name.clone(), tbl) {
            Ok(_) => {}
            Err(err) => match err {
                StoreError::AlreadyExists(_) => {}
                other => return Err(other.into()),
            },
        }
    }

    info!("metrics initialization...");
    init_metrics();
    info!("initializing session cleaner...");
    init_session_cleaner(md.clone(), db.clone(), cfg.clone())?;
    info!("initializing backup...");
    backup::init(md.clone(), db.clone(), cfg.clone()).await?;

    Ok(())
}

fn init_platform(
    md: Arc<MetadataProvider>,
    db: Arc<OptiDBImpl>,
    router: Router,
    cfg: Config,
) -> crate::error::Result<Router> {
    let es_provider = Arc::new(EventSegmentationProvider::new(md.clone(), db.clone()));
    let funnel_provider = Arc::new(FunnelProvider::new(md.clone(), db.clone()));
    let prop_provider = Arc::new(PropertiesProvider::new(md.clone(), db.clone()));
    let er_provider = Arc::new(EventRecordsProvider::new(md.clone(), db.clone()));
    let gr_provider = Arc::new(GroupRecordsProvider::new(md.clone(), db.clone()));
    let platform_provider = Arc::new(PlatformProvider::new(
        md.clone(),
        es_provider,
        funnel_provider,
        prop_provider,
        er_provider,
        gr_provider,
        cfg.clone(),
    ));

    info!("attaching platform routes...");
    let router = platform::http::attach_routes(router, &md, &platform_provider, cfg);

    Ok(router)
}

fn init_ingester(
    geo_city_path: &PathBuf,
    ua_db_path: &PathBuf,
    md: &Arc<MetadataProvider>,
    db: &Arc<OptiDBImpl>,
    router: Router,
) -> crate::error::Result<Router> {
    let mut track_transformers = Vec::new();

    info!("initializing ua parser...");
    let ua_parser = UserAgentParser::from_file(File::open(ua_db_path)?)
        .map_err(|e| Error::Internal(e.to_string()))?;
    let ua = user_agent::track::UserAgent::try_new(md.event_properties.clone(), ua_parser)?;
    track_transformers.push(Arc::new(ua) as Arc<dyn Transformer<Track>>);

    info!("initializing geo...");
    let city_rdr = maxminddb::Reader::open_readfile(geo_city_path)?;
    let geo = geo::track::Geo::try_new(md.event_properties.clone(), city_rdr)?;
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

    let mut identify_destinations = Vec::new();
    let identify_dst = ingester::destinations::local::identify::Local::new(db.clone(), md.clone());
    identify_destinations.push(Arc::new(identify_dst) as Arc<dyn Destination<Identify>>);
    let identify_exec =
        Executor::<Identify>::new(vec![], identify_destinations, db.clone(), md.clone());

    info!("attaching ingester routes...");
    Ok(ingester::sources::http::attach_routes(
        router,
        track_exec,
        identify_exec,
    ))
}

fn init_session_cleaner(
    md: Arc<MetadataProvider>,
    db: Arc<OptiDBImpl>,
    cfg: Config,
) -> crate::error::Result<()> {
    thread::spawn(move || {
        loop {
            thread::sleep(cfg.misc.session_cleaner_interval.to_std().unwrap());
            for project in md.projects.list().unwrap() {
                md.sessions
                    .check_for_deletion(project.id, |sess| {
                        let now = Utc::now();
                        let sess_len = now - sess.created_at;
                        if sess_len.num_seconds() < project.session_duration_seconds as i64 {
                            return Ok(false);
                        }
                        let groups = (1..GROUPS_COUNT)
                            .map(|gid| NamedValue::new(group_col(gid), Value::Int64(None)))
                            .collect::<Vec<_>>();

                        let record_id = md.events.next_record_sequence(project.id).unwrap();

                        let event_id = md
                            .events
                            .get_by_name(project.id, EVENT_SESSION_END)
                            .unwrap()
                            .id;

                        let values = [
                            vec![
                                NamedValue::new(
                                    COLUMN_PROJECT_ID.to_string(),
                                    Value::Int64(Some(project.id as i64)),
                                ),
                                NamedValue::new(
                                    group_col(GROUP_USER_ID),
                                    Value::Int64(Some(sess.user_id as i64)),
                                ),
                            ],
                            groups.clone(),
                            vec![
                                NamedValue::new(
                                    COLUMN_CREATED_AT.to_string(),
                                    Value::Timestamp(Some(now.timestamp())),
                                ),
                                NamedValue::new(
                                    COLUMN_EVENT_ID.to_string(),
                                    Value::Int64(Some(record_id as i64)),
                                ),
                                NamedValue::new(
                                    COLUMN_EVENT.to_string(),
                                    Value::Int64(Some(event_id as i64)),
                                ),
                                NamedValue::new(
                                    md.event_properties
                                        .get_by_name(project.id, EVENT_PROPERTY_SESSION_LENGTH)
                                        .unwrap()
                                        .column_name(),
                                    Value::Timestamp(Some(sess_len.num_seconds())),
                                ),
                            ],
                        ]
                        .concat();

                        db.insert(TABLE_EVENTS, values).unwrap();

                        Ok(true)
                    })
                    .unwrap();
            }
        }
    });

    Ok(())
}

// not for high load
fn get_random_key64(rng: &mut StdRng) -> [u8; 64] {
    let mut arr = [0u8; 64];
    rng.try_fill(&mut arr[..]).unwrap();
    arr
}

fn init_settings(md: &Arc<MetadataProvider>) -> Result<()> {
    let mut rng = StdRng::from_rng(rand::thread_rng())?;
    let mut settings = match md.settings.load() {
        Ok(cfg) => cfg,
        Err(MetadataError::NotFound(_)) => {
            let cfg = metadata::settings::Settings::default();
            md.settings.save(&cfg)?;
            cfg
        }
        Err(err) => return Err(err.into()),
    };

    if settings.auth_access_token.is_empty() {
        let key = hex::encode(get_random_key64(&mut rng));
        settings.auth_access_token = key;
    }
    if settings.auth_refresh_token.is_empty() {
        let key = hex::encode(get_random_key64(&mut rng));
        settings.auth_refresh_token = key;
    }

    md.settings.save(&settings)?;
    Ok(())
}

fn init_test_org_structure(md: &Arc<MetadataProvider>) -> crate::error::Result<Project> {
    let admin = match md.accounts.create(CreateAccountRequest {
        created_by: ADMIN_ID,
        password_hash: make_password_hash("admin")?,
        email: "admin@admin.com".to_string(),
        name: Some("admin".to_string()),
        force_update_password: true,
        force_update_email: true,
        role: Some(Role::Admin),
        organizations: None,
        projects: None,
        teams: None,
    }) {
        Ok(acc) => acc,
        Err(_err) => md.accounts.get_by_email("admin@admin.com")?,
    };
    let org = match md.organizations.create(CreateOrganizationRequest {
        created_by: admin.id,
        name: "My Organization".to_string(),
    }) {
        Ok(org) => org,
        Err(_err) => md.organizations.get_by_id(1)?,
    };

    let token = Alphanumeric.sample_string(&mut thread_rng(), 64);

    let proj = match md.projects.create(CreateProjectRequest {
        created_by: admin.id,
        organization_id: org.id,
        name: "My Project".to_string(),
        description: None,
        tags: None,
        token: token.clone(),
        session_duration_seconds: 60 * 60 * 24,
    }) {
        Ok(proj) => proj,
        Err(_err) => md.projects.get_by_id(1)?,
    };
    md.dictionaries.create_key(
        proj.id,
        TABLE_EVENTS,
        "project_id",
        proj.id,
        proj.name.as_str(),
    )?;
    for g in 0..GROUPS_COUNT {
        md.dictionaries.create_key(
            proj.id,
            group_col(g).as_str(),
            "project_id",
            proj.id,
            proj.name.as_str(),
        )?;
    }

    info!("project token: {}", proj.token);
    let _user = match md.accounts.create(CreateAccountRequest {
        created_by: admin.id,
        password_hash: make_password_hash("test")?,
        email: "user@test.com".to_string(),
        name: Some("user".to_string()),
        force_update_password: true,
        force_update_email: true,
        role: None,
        organizations: Some(vec![(org.id, OrganizationRole::Member)]),
        projects: Some(vec![(proj.id, ProjectRole::Reader)]),
        teams: None,
    }) {
        Ok(acc) => acc,
        Err(_err) => md.accounts.get_by_email("user@test.com")?,
    };

    Ok(proj.clone())
}
