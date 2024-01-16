pub mod accounts;
pub mod auth;
pub mod custom_events;
pub mod dashboards;
pub mod event_records;
pub mod events;
pub mod group_records;
pub mod properties;
pub mod queries;
pub mod reports;

use std::path::PathBuf;
use std::sync::Arc;

use axum::middleware;
use axum::Extension;
use axum::Router;
use metadata::MetadataProvider;
use tower_cookies::CookieManagerLayer;
use tower_http::cors::Any;
use tower_http::cors::CorsLayer;
use tower_http::services::ServeDir;
use tower_http::services::ServeFile;
use tower_http::trace::TraceLayer;
use tracing::info;

use crate::auth::provider::Config;
use crate::context::print_request_response;
use crate::properties::Properties;
use crate::PlatformProvider;

pub struct Service {}

#[derive(Clone)]
pub struct PropertiesLayer {
    event: Arc<Properties>,
    user: Arc<Properties>,
    system: Arc<Properties>,
}

pub fn attach_routes(
    mut router: Router,
    md: &Arc<MetadataProvider>,
    platform: &Arc<PlatformProvider>,
    auth_cfg: Config,
    ui: Option<PathBuf>,
) -> Router {
    router = accounts::attach_routes(router);
    router = auth::attach_routes(router);
    router = events::attach_routes(router);
    router = custom_events::attach_routes(router);
    router = properties::attach_event_routes(router);
    router = properties::attach_user_routes(router);
    router = properties::attach_system_routes(router);
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
        .layer(Extension(PropertiesLayer {
            event: platform.event_properties.clone(),
            user: platform.user_properties.clone(),
            system: platform.system_properties.clone(),
        }))
        .layer(Extension(auth_cfg))
        .layer(Extension(platform.query.clone()))
        .layer(Extension(platform.dashboards.clone()))
        .layer(Extension(platform.reports.clone()));
    // .layer(Extension(platform.event_records.clone()))
    // .layer(Extension(platform.group_records.clone()));

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
