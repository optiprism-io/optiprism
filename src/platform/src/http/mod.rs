pub mod accounts;
pub mod auth;
pub mod custom_events;
pub mod custom_properties;
pub mod dashboards;
pub mod event_records;
pub mod events;
pub mod group_records;
pub mod properties;
pub mod queries;
pub mod reports;

use std::collections::BTreeMap;
use std::error::Error;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use axum::async_trait;
use axum::body::HttpBody;
use axum::extract::rejection::JsonRejection;
use axum::http;
use axum::http::HeaderMap;
use axum::http::HeaderValue;
use axum::http::Request;
use axum::middleware;
use axum::middleware::Next;
use axum::Extension;
use axum::Router;
use axum::Server;
use axum_core::body;
use axum_core::extract::FromRequest;
use axum_core::response::IntoResponse;
use axum_core::response::Response;
use axum_core::BoxError;
use bytes::Bytes;
use hyper::Body;
use lazy_static::lazy_static;
use metadata::MetadataProvider;
use regex::Regex;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;
use tokio::select;
use tokio::signal::unix::SignalKind;
use tower_cookies::CookieManagerLayer;
use tower_http::cors::Any;
use tower_http::cors::CorsLayer;
use tower_http::services::ServeDir;
use tower_http::services::ServeFile;
use tower_http::trace::TraceLayer;
use tracing::info;

use crate::context::print_request_response;
use crate::PlatformError;
use crate::PlatformProvider;
use crate::Result;

pub struct Service {
    router: Router,
    addr: SocketAddr,
}

pub fn attach_routes(mut router: Router, md: &Arc<MetadataProvider>,
                     platform: &Arc<PlatformProvider>,
                     auth_cfg: crate::auth::Config, ui: Option<PathBuf>) -> Router {
    info!("attaching platform routes...");
    router = accounts::attach_routes(router);
    router = auth::attach_routes(router);
    router = events::attach_routes(router);
    router = custom_events::attach_routes(router);
    router = properties::attach_event_routes(router);
    router = properties::attach_user_routes(router);
    router = custom_properties::attach_routes(router);
    router = queries::attach_routes(router);
    router = dashboards::attach_routes(router);
    router = reports::attach_routes(router);
    router = event_records::attach_routes(router);
    router = group_records::attach_routes(router);
    if let Some(path) = ui {
        info!("attaching ui static files handler...");
        router = router.nest_service(
            "/assets",
            ServeDir::new(path.join("assets")).not_found_service(ServeFile::new("index.html")),
        );
    }
    // fixme get rid of cloning
    router = router.clone().nest("/api/v1", router);
    router = router
        .layer(Extension(md.accounts.clone()))
        .layer(Extension(platform.accounts.clone()))
        .layer(Extension(platform.auth.clone()))
        .layer(Extension(platform.events.clone()))
        .layer(Extension(platform.custom_events.clone()))
        .layer(Extension(platform.event_properties.clone()))
        .layer(Extension(platform.user_properties.clone()))
        .layer(Extension(platform.custom_properties.clone()))
        .layer(Extension(auth_cfg))
        .layer(Extension(platform.query.clone()))
        .layer(Extension(platform.dashboards.clone()))
        .layer(Extension(platform.reports.clone()))
        .layer(Extension(platform.event_records.clone()))
        .layer(Extension(platform.group_records.clone()));

    router = router
        .layer(CookieManagerLayer::new())
        .layer(TraceLayer::new_for_http())
        .layer(middleware::from_fn(print_request_response));

    let cors = CorsLayer::new()
        .allow_methods(Any)
        .allow_origin(Any)
        .allow_headers(Any);

    router = router.layer(cors);

    router
}