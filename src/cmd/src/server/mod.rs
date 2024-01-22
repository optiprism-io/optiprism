mod config;

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
use common::rbac::Role;
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
use metadata::accounts::CreateAccountRequest;
use metadata::error::MetadataError;
use metadata::MetadataProvider;
use platform::auth::password::make_password_hash;
use rand::thread_rng;
use store::db::OptiDBImpl;
use store::db::Options;
use store::NamedValue;
use store::Value;
use tokio::select;
use tokio::signal::unix::SignalKind;
use tracing::debug;
use tracing::info;

use crate::error::Error;
use crate::error::Result;
use crate::init_ingester;
use crate::init_metrics;
use crate::init_platform;
use crate::init_project;
use crate::init_session_cleaner;
use crate::init_system;
pub use crate::server::config::Config;

pub async fn start(cfg: Config) -> Result<()> {
    debug!("db path: {:?}", cfg.path);

    let rocks = Arc::new(metadata::rocksdb::new(cfg.path.join("md"))?);
    let db = Arc::new(OptiDBImpl::open(cfg.path.join("store"), Options {})?);
    let md = Arc::new(MetadataProvider::try_new(rocks, db.clone())?);
    info!("metrics initialization...");
    init_metrics();
    info!("system initialization...");
    init_system(&md, &db, num_cpus::get())?;

    if let Some(ui_path) = &cfg.ui_path {
        if !ui_path.try_exists()? {
            return Err(Error::FileNotFound(format!(
                "ui path {ui_path:?} doesn't exist"
            )));
        }
        debug!("ui path: {:?}", ui_path);
    }

    let just_initialized = if !md.accounts.list()?.is_empty() {
        info!("creating admin account...");
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
            Err(MetadataError::AlreadyExists(_)) => {}
            Err(err) => return Err(err.into()),
            _ => {}
        };
        true
    } else {
        false
    };

    let router = Router::new();
    info!("initializing session cleaner...");
    init_session_cleaner(md.clone(), db.clone())?;
    info!("initializing ingester...");
    let router = init_ingester(&cfg.geo_city_path, &cfg.ua_db_path, &md, &db, router)?;
    info!("initializing platform...");
    let router = init_platform(md.clone(), db.clone(), router)?;

    let server = axum::Server::bind(&cfg.host)
        .serve(router.into_make_service_with_connect_info::<SocketAddr>());
    info!(
        "Web Interface: https://{} (email: admin@admin.com, password: admin, DON'T FORGET TO CHANGE)",
        cfg.host
    );
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
