mod accounts;
pub mod auth;
mod custom_events;
mod dashboards;
mod event_records;
mod events;
mod group_records;
mod groups;
mod organizations;
mod projects;
mod properties;
mod reports;
mod event_segmentation;
mod funnel;
mod bookmarks;
mod backups;
mod settings;
use std::sync::Arc;

use axum::middleware;
use axum::Extension;
use axum::Router;
use common::config::Config;
use common::http::{measure_request_response, print_request_response};
use metadata::MetadataProvider;
use tower_cookies::CookieManagerLayer;
use tower_http::cors::Any;
use tower_http::cors::CorsLayer;
use tower_http::services::ServeDir;
use tower_http::services::ServeFile;
use tower_http::trace::TraceLayer;

use crate::properties::Properties;
use crate::PlatformProvider;

pub struct Service {}

#[derive(Clone)]
pub struct PropertiesLayer {
    events: Arc<Properties>,
    groups: Vec<Arc<Properties>>,
}

pub fn attach_routes(
    mut router: Router,
    md: &Arc<MetadataProvider>,
    platform: &Arc<PlatformProvider>,
    cfg: Config,
) -> Router {
    router = organizations::attach_routes(router);
    router = projects::attach_routes(router);
    router = accounts::attach_routes(router);
    router = backups::attach_routes(router);
    router = settings::attach_routes(router);
    router = auth::attach_routes(router);
    router = events::attach_routes(router);
    router = custom_events::attach_routes(router);
    router = groups::attach_routes(router);
    router = properties::attach_routes(router);
    router = properties::attach_event_routes(router);
    router = properties::attach_group_routes(router);
    router = dashboards::attach_routes(router);
    router = reports::attach_routes(router);
    router = bookmarks::attach_routes(router);
    router = event_records::attach_routes(router);
    router = event_segmentation::attach_routes(router);
    router = funnel::attach_routes(router);
    router = group_records::attach_routes(router);
    let serve_dir = ServeDir::new(&cfg.data.ui_path)
        .not_found_service(ServeFile::new(cfg.data.ui_path.join("index.html")));
    router = router
        .nest_service("/", serve_dir.clone())
        .fallback_service(serve_dir);

    router = router
        .layer(Extension(platform.organizations.clone()))
        .layer(Extension(platform.projects.clone()))
        .layer(Extension(platform.backups.clone()))
        .layer(Extension(platform.settings.clone()))
        .layer(Extension(md.accounts.clone()))
        .layer(Extension(md.settings.clone()))
        .layer(Extension(platform.accounts.clone()))
        .layer(Extension(platform.auth.clone()))
        .layer(Extension(platform.events.clone()))
        .layer(Extension(platform.custom_events.clone()))
        .layer(Extension(platform.groups.clone()))
        .layer(Extension(PropertiesLayer {
            events: platform.event_properties.clone(),
            groups: platform.group_properties.clone(),
        }))
        .layer(Extension(cfg))
        .layer(Extension(platform.event_segmentation.clone()))
        .layer(Extension(platform.funnel.clone()))
        .layer(Extension(platform.dashboards.clone()))
        .layer(Extension(platform.reports.clone()))
        .layer(Extension(platform.bookmarks.clone()))
        .layer(Extension(platform.event_records.clone()))
        .layer(Extension(platform.group_records.clone()));

    let cors = CorsLayer::new()
        .allow_methods(Any)
        .allow_origin(Any)
        .allow_headers(Any);

    router = router
        .layer(cors)
        .layer(CookieManagerLayer::new())
        .layer(Extension(TraceLayer::new_for_http()))
        .layer(middleware::from_fn(measure_request_response))
        .layer(middleware::from_fn(print_request_response));

    router
}
