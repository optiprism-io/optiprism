use std::fs;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use common::config::Config;
use common::rbac::Role;
use common::{ADMIN_ID, DATA_PATH_METADATA, DATA_PATH_STORAGE};
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
use crate::{init_config, init_ingester};
use crate::init_metrics;
use crate::init_platform;
use crate::init_session_cleaner;
use crate::init_system;

pub async fn start(mut cfg: Config) -> Result<()> {
    debug!("db path: {:?}", cfg.data.path);

    fs::create_dir_all(cfg.data.path.join(DATA_PATH_METADATA))?;
    fs::create_dir_all(cfg.data.path.join(DATA_PATH_STORAGE))?;
    let rocks = Arc::new(metadata::rocksdb::new(cfg.data.path.join(DATA_PATH_METADATA))?);
    let db = Arc::new(OptiDBImpl::open(cfg.data.path.join(DATA_PATH_STORAGE), Options {})?);
    let md = Arc::new(MetadataProvider::try_new(rocks, db.clone())?);
    info!("metrics initialization...");
    init_metrics();
    info!("system initialization...");
    init_system(&md, &db, num_cpus::get())?;
    init_config(&md,&mut cfg)?;
    if let Some(ui_path) = &cfg.data.ui_path {
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
    let router = init_ingester(&cfg.data.geo_city_path, &cfg.data.ua_db_path, &md, &db, router)?;
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

    info!("Web Interface: https://{}", cfg.server.host);
    if just_initialized {
        info!("email: admin@admin.com, password: admin, (DON'T FORGET TO CHANGE)");
    }
    let listener = tokio::net::TcpListener::bind(cfg.server.host).await?;
    Ok(axum::serve(
        listener,
        router.into_make_service_with_connect_info::<SocketAddr>(),
    )
        .await?)
}
