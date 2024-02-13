use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;

use ::storage::db::OptiDBImpl;
use ::storage::error::StoreError;
use ::storage::table::Options as TableOptions;
use ::storage::NamedValue;
use ::storage::Value;
use axum::Router;
use chrono::Duration;
use chrono::Utc;
use common::config::Config;
use common::defaults::SESSION_DURATION;
use common::rbac::OrganizationRole;
use common::rbac::ProjectRole;
use common::rbac::Role;
use common::types::DType;
use common::types::COLUMN_CREATED_AT;
use common::types::COLUMN_EVENT;
use common::types::COLUMN_EVENT_ID;
use common::types::COLUMN_PROJECT_ID;
use common::types::COLUMN_USER_ID;
use common::types::EVENT_CLICK;
use common::types::EVENT_PAGE;
use common::types::EVENT_PROPERTY_A_CLASS;
use common::types::EVENT_PROPERTY_A_HREF;
use common::types::EVENT_PROPERTY_A_ID;
use common::types::EVENT_PROPERTY_A_NAME;
use common::types::EVENT_PROPERTY_A_STYLE;
use common::types::EVENT_PROPERTY_PAGE_PATH;
use common::types::EVENT_PROPERTY_PAGE_REFERER;
use common::types::EVENT_PROPERTY_PAGE_SEARCH;
use common::types::EVENT_PROPERTY_PAGE_TITLE;
use common::types::EVENT_PROPERTY_PAGE_URL;
use common::types::EVENT_PROPERTY_SESSION_LENGTH;
use common::types::EVENT_SCREEN;
use common::types::EVENT_SESSION_BEGIN;
use common::types::EVENT_SESSION_END;
use common::types::TABLE_EVENTS;
use common::types::USER_PROPERTY_CITY;
use common::types::USER_PROPERTY_CLIENT_FAMILY;
use common::types::USER_PROPERTY_CLIENT_VERSION_MAJOR;
use common::types::USER_PROPERTY_CLIENT_VERSION_MINOR;
use common::types::USER_PROPERTY_CLIENT_VERSION_PATCH;
use common::types::USER_PROPERTY_COUNTRY;
use common::types::USER_PROPERTY_DEVICE_BRAND;
use common::types::USER_PROPERTY_DEVICE_FAMILY;
use common::types::USER_PROPERTY_DEVICE_MODEL;
use common::types::USER_PROPERTY_OS;
use common::types::USER_PROPERTY_OS_FAMILY;
use common::types::USER_PROPERTY_OS_VERSION_MAJOR;
use common::types::USER_PROPERTY_OS_VERSION_MINOR;
use common::types::USER_PROPERTY_OS_VERSION_PATCH;
use common::types::USER_PROPERTY_OS_VERSION_PATCH_MINOR;
use ingester::error::IngesterError;
use ingester::executor::Executor;
use ingester::transformers::geo;
use ingester::transformers::user_agent;
use ingester::Destination;
use ingester::Identify;
use ingester::Track;
use ingester::Transformer;
use metadata::accounts::Account;
use metadata::accounts::CreateAccountRequest;
use metadata::organizations::CreateOrganizationRequest;
use metadata::organizations::Organization;
use metadata::projects::CreateProjectRequest;
use metadata::projects::Project;
use metadata::properties::DictionaryType;
use metadata::properties::Type;
use metadata::util::create_event;
use metadata::util::create_property;
use metadata::util::CreatePropertyMainRequest;
use metadata::MetadataProvider;
use metrics::describe_counter;
use metrics::describe_histogram;
use metrics::Unit;
use metrics_exporter_prometheus::PrometheusBuilder;
use platform::auth;
use platform::auth::password::make_password_hash;
use platform::PlatformProvider;
use query::QueryProvider;
use tracing::info;
use uaparser::UserAgentParser;

use crate::error::Error;

pub mod error;
pub mod server;
pub mod store;
pub mod test;

pub fn init_metrics() {
    let builder = PrometheusBuilder::new();
    builder
        .install()
        .expect("failed to install Prometheus recorder");

    describe_counter!("store.inserts_total", "number of inserts processed");
    describe_histogram!("store.insert_time_seconds", Unit::Seconds, "insert time");
    describe_counter!("store.scans_total", "number of scans processed");
    describe_counter!("store.scan_merges_total", "number of merges during scan");
    describe_histogram!("store.scan_time_seconds", Unit::Microseconds, "scan time");
    describe_histogram!(
        "store.scan_memtable_seconds",
        Unit::Seconds,
        "scan memtable time"
    );
    describe_counter!("store.compactions_total", "number of compactions");
    describe_histogram!(
        "store.compaction_time_seconds",
        Unit::Seconds,
        "compaction time"
    );
    describe_histogram!(
        "store.recovery_time_seconds",
        Unit::Seconds,
        "recovery time"
    );

    describe_histogram!("store.flush_time_seconds", Unit::Seconds, "recovery time");
}

pub fn init_system(
    md: &Arc<MetadataProvider>,
    db: &Arc<OptiDBImpl>,
    partitions: usize,
) -> error::Result<()> {
    let topts = TableOptions {
        levels: 7,
        merge_array_size: 10000,
        parallelism: partitions,
        index_cols: 2,
        l1_max_size_bytes: 1024 * 1024 * 10,
        level_size_multiplier: 10,
        l0_max_parts: 4,
        max_log_length_bytes: 1024 * 1024 * 100,
        merge_array_page_size: 100000,
        merge_data_page_size_limit_bytes: Some(1024 * 1024 * 1000),
        merge_index_cols: 2,
        merge_max_l1_part_size_bytes: 1024 * 1024 * 10,
        merge_part_size_multiplier: 10,
        merge_row_group_values_limit: 1000,
        merge_chunk_size: 1024 * 8 * 8,
        merge_max_page_size: 1024 * 1024 * 10,
    };
    match db.create_table(TABLE_EVENTS.to_string(), topts) {
        Ok(_) => {}
        Err(err) => match err {
            StoreError::AlreadyExists(_) => {}
            other => return Err(other.into()),
        },
    }

    create_property(md, 0, CreatePropertyMainRequest {
        name: COLUMN_PROJECT_ID.to_string(),
        typ: Type::System,
        data_type: DType::Int64,
        nullable: false,
        dict: None,
    })?;

    create_property(md, 0, CreatePropertyMainRequest {
        name: COLUMN_USER_ID.to_string(),
        typ: Type::System,
        data_type: DType::Int64,
        nullable: false,
        dict: None,
    })?;

    create_property(md, 0, CreatePropertyMainRequest {
        name: COLUMN_CREATED_AT.to_string(),
        typ: Type::System,
        data_type: DType::Timestamp,
        nullable: false,
        dict: None,
    })?;

    create_property(md, 0, CreatePropertyMainRequest {
        name: COLUMN_EVENT_ID.to_string(),
        typ: Type::System,
        data_type: DType::Int64,
        nullable: false,
        dict: None,
    })?;

    create_property(md, 0, CreatePropertyMainRequest {
        name: COLUMN_EVENT.to_string(),
        typ: Type::System,
        data_type: DType::String,
        nullable: false,
        dict: Some(DictionaryType::Int64),
    })?;

    Ok(())
}

fn init_platform(
    md: Arc<MetadataProvider>,
    db: Arc<OptiDBImpl>,
    router: Router,
    cfg: Config,
) -> crate::error::Result<Router> {
    let query_provider = Arc::new(QueryProvider::new(md.clone(), db.clone()));

    let platform_provider = Arc::new(PlatformProvider::new(
        md.clone(),
        query_provider,
        cfg.clone(),
    ));

    info!("attaching platform routes...");
    Ok(platform::http::attach_routes(
        router,
        &md,
        &platform_provider,
        cfg,
        None,
    ))
}

fn init_ingester(
    geo_city_path: &PathBuf,
    ua_db_path: &PathBuf,
    md: &Arc<MetadataProvider>,
    db: &Arc<OptiDBImpl>,
    router: Router,
) -> crate::error::Result<Router> {
    let mut track_transformers = Vec::new();
    let ua_parser = UserAgentParser::from_file(File::open(ua_db_path)?)
        .map_err(|e| Error::Internal(e.to_string()))?;
    let ua = user_agent::track::UserAgent::try_new(md.user_properties.clone(), ua_parser)?;
    track_transformers.push(Arc::new(ua) as Arc<dyn Transformer<Track>>);

    // todo make common
    let city_rdr = maxminddb::Reader::open_readfile(geo_city_path)?;
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
    let ua_parser = UserAgentParser::from_file(File::open(ua_db_path)?)
        .map_err(|e| IngesterError::Internal(e.to_string()))?;
    let ua = user_agent::identify::UserAgent::try_new(md.user_properties.clone(), ua_parser)?;
    identify_transformers.push(Arc::new(ua) as Arc<dyn Transformer<Identify>>);

    info!("initializing geo...");
    let city_rdr = maxminddb::Reader::open_readfile(geo_city_path)?;
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

fn init_session_cleaner(
    md: Arc<MetadataProvider>,
    db: Arc<OptiDBImpl>,
    cfg: Config,
) -> crate::error::Result<()> {
    thread::spawn(move || {
        loop {
            thread::sleep(cfg.session_cleaner_interval.to_std().unwrap());
            for project in md.projects.list(None).unwrap() {
                md.sessions
                    .check_for_deletion(project.id, |sess| {
                        let now = Utc::now();
                        let sess_len = now - sess.created_at;
                        if sess_len.num_seconds() < project.session_duration_seconds as i64 {
                            return Ok(false);
                        }
                        let record_id = md.events.next_record_sequence(project.id).unwrap();

                        let event_id = md
                            .events
                            .get_by_name(project.id, EVENT_SESSION_END)
                            .unwrap()
                            .id;

                        let values = vec![
                            NamedValue::new(
                                COLUMN_PROJECT_ID.to_string(),
                                Value::Int64(Some(project.id as i64)),
                            ),
                            NamedValue::new(
                                COLUMN_USER_ID.to_string(),
                                Value::Int64(Some(sess.user_id as i64)),
                            ),
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
                        ];

                        db.insert("events", values).unwrap();

                        Ok(true)
                    })
                    .unwrap();
            }
        }
    });

    Ok(())
}

fn init_test_org_structure(md: &Arc<MetadataProvider>) -> crate::error::Result<u64> {
    let admin = match md.accounts.create(CreateAccountRequest {
        created_by: None,
        password_hash: make_password_hash("admin")?,
        email: "admin@admin.com".to_string(),
        name: Some("admin".to_string()),
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

    let proj = match md.projects.create(CreateProjectRequest {
        created_by: admin.id,
        organization_id: org.id,
        name: "My Project".to_string(),
        description: None,
        tags: None,
        session_duration_seconds: SESSION_DURATION as u64,
    }) {
        Ok(proj) => proj,
        Err(_err) => md.projects.get_by_id(1)?,
    };

    info!("token: {}", proj.token);
    let _user = match md.accounts.create(CreateAccountRequest {
        created_by: Some(admin.id),
        password_hash: make_password_hash("test")?,
        email: "user@test.com".to_string(),
        name: Some("user".to_string()),
        role: None,
        organizations: Some(vec![(org.id, OrganizationRole::Member)]),
        projects: Some(vec![(proj.id, ProjectRole::Reader)]),
        teams: None,
    }) {
        Ok(acc) => acc,
        Err(_err) => md.accounts.get_by_email("user@test.com")?,
    };

    Ok(proj.id)
}
