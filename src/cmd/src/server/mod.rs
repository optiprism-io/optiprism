pub mod config;

use std::fs;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use common::config::Config;
use common::defaults::SESSION_DURATION;
use common::rbac::Role;
use hyper::Server;
use metadata::accounts::CreateAccountRequest;
use metadata::error::MetadataError;
use metadata::organizations::CreateOrganizationRequest;
use metadata::projects::CreateProjectRequest;
use metadata::MetadataProvider;
use platform::auth::password::make_password_hash;
use storage::db::OptiDBImpl;
use storage::db::Options;
use tokio::net::TcpListener;
use tokio::select;
use tokio::signal::unix::SignalKind;
use tracing::debug;
use tracing::info;

use crate::error::Error;
use crate::error::Result;
use crate::init_ingester;
use crate::init_metrics;
use crate::init_platform;
use crate::init_session_cleaner;
use crate::init_system;

pub async fn start(cfg: Config) -> Result<()> {
    debug!("db path: {:?}", cfg.path);

    fs::create_dir_all(cfg.path.join("data/md"))?;
    fs::create_dir_all(cfg.path.join("data/storage"))?;
    let rocks = Arc::new(metadata::rocksdb::new(cfg.path.join("data/md"))?);
    let db = Arc::new(OptiDBImpl::open(cfg.path.join("data/storage"), Options {})?);
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

    let just_initialized = if md.accounts.list()?.is_empty() {
        info!("creating admin account...");
        let acc = md.accounts.create(CreateAccountRequest {
            created_by: None,
            password_hash: make_password_hash("admin")?,
            email: "admin@admin.com".to_string(),
            name: Some("admin".to_string()),
            role: Some(Role::Admin),
            organizations: None,
            projects: None,
            teams: None,
        })?;

        info!("creating organization...");
        md.organizations.create(CreateOrganizationRequest {
            created_by: acc.id,
            name: "My Organization".to_string(),
        })?;

        true
    } else {
        false
    };

    let router = Router::new();
    info!("initializing session cleaner...");
    init_session_cleaner(md.clone(), db.clone(), cfg.clone())?;
    info!("initializing ingester...");
    let router = init_ingester(&cfg.geo_city_path, &cfg.ua_db_path, &md, &db, router)?;
    info!("initializing platform...");
    let router = init_platform(md.clone(), db.clone(), router, cfg.clone())?;

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

    info!("Web Interface: https://{}", cfg.host);
    if just_initialized {
        info!("email: admin@admin.com, password: admin, (DON'T FORGET TO CHANGE)");
    }
    Ok(axum::serve(TcpListener::bind(&cfg.host).await?, router)
        .with_graceful_shutdown(signal)
        .await?)
}
