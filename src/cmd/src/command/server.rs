use std::fs;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use common::startup_config::StartupConfig;
use common::rbac::Role;
use common::{ADMIN_ID, DATA_PATH_METADATA, DATA_PATH_STORAGE};
use hyper::Server;
use rand::distributions::Alphanumeric;
use rand::Rng;
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
use crate::{backup, init_config, init_ingester};
use crate::init_metrics;
use crate::init_platform;
use crate::init_session_cleaner;
use crate::init_system;

pub async fn start(mut cfg: StartupConfig) -> Result<()> {
    debug!("db path: {:?}", cfg.data.path);

    fs::create_dir_all(cfg.data.path.join(DATA_PATH_METADATA))?;
    fs::create_dir_all(cfg.data.path.join(DATA_PATH_STORAGE))?;
    let rocks = Arc::new(metadata::rocksdb::new(cfg.data.path.join(DATA_PATH_METADATA))?);
    let db = Arc::new(OptiDBImpl::open(cfg.data.path.join(DATA_PATH_STORAGE), Options {})?);
    let md = Arc::new(MetadataProvider::try_new(rocks, db.clone())?);
    init_config(&md, &mut cfg)?;

    info!("system initialization...");
    init_system(&md, &db, &cfg).await?;
    if !cfg.data.ui_path.try_exists()? {
        return Err(Error::FileNotFound(format!(
            "ui path {:?} doesn't exist", cfg.data.ui_path
        )));
    }
    debug!("ui path: {:?}", cfg.data.ui_path);

    let admin_acc = match md.accounts.get_by_id(ADMIN_ID) {
        Ok(acc) => acc,
        Err(err) => match err {
            MetadataError::NotFound(_) => {
                let pwd: String = rand::thread_rng()
                    .sample_iter(&Alphanumeric)
                    .take(32)
                    .map(char::from)
                    .collect();
                let mut sys_cfg = md.config.load()?;
                sys_cfg.auth.admin_default_password = pwd.clone();
                md.config.save(&sys_cfg)?;
                info!("creating admin account...");
                let acc = md.accounts.create(CreateAccountRequest {
                    created_by: ADMIN_ID,
                    password_hash: make_password_hash(&pwd)?,
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

                acc
            }
            other => return Err(other.into()),
        }
    };
    let router = Router::new();
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
    if admin_acc.force_update_email {
        info!("email: {}",admin_acc.email);
    }
    if admin_acc.force_update_password {
        let pwd = md.config.load()?.auth.admin_default_password;
        info!("password: {}",pwd);
    }
    let listener = tokio::net::TcpListener::bind(cfg.server.host).await?;
    Ok(axum::serve(
        listener,
        router.into_make_service_with_connect_info::<SocketAddr>(),
    )
        .await?)
}
