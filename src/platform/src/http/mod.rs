pub mod accounts;
pub mod auth;
pub mod custom_events;
pub mod debug;
pub mod events;
pub mod json;
pub mod properties;
pub mod queries;

use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use axum::http::StatusCode;
use axum::middleware;
use axum::routing::get_service;
use axum::Extension;
use axum::Router;
use axum::Server;
use metadata::MetadataProvider;
use tokio::select;
use tokio::signal::unix::SignalKind;
use tower_cookies::CookieManagerLayer;
use tower_http::services::ServeDir;
use tower_http::services::ServeFile;
use tower_http::trace::TraceLayer;
use tracing::info;

use crate::error::Result;
use crate::http::debug::print_request_response;
use crate::PlatformProvider;

pub struct Service {
    router: Router,
    addr: SocketAddr,
}

impl Service {
    pub fn new(
        md: &Arc<MetadataProvider>,
        platform: &Arc<PlatformProvider>,
        addr: SocketAddr,
        _ui_path: Option<PathBuf>,
    ) -> Self {
        let mut router = Router::new();

        info!("attaching api routes...");
        router = accounts::attach_routes(router);
        router = auth::attach_routes(router);
        router = events::attach_routes(router);
        router = custom_events::attach_routes(router);
        router = properties::attach_event_routes(router);
        router = properties::attach_user_routes(router);
        router = queries::attach_routes(router);
        router = router.clone().nest("/api/v1", router);

        router = router
            .layer(Extension(md.accounts.clone()))
            .layer(Extension(platform.accounts.clone()))
            .layer(Extension(platform.auth.clone()))
            .layer(Extension(platform.events.clone()))
            .layer(Extension(platform.custom_events.clone()))
            .layer(Extension(platform.event_properties.clone()))
            .layer(Extension(platform.user_properties.clone()))
            .layer(Extension(platform.query.clone()));

        router = router
            .layer(CookieManagerLayer::new())
            .layer(TraceLayer::new_for_http())
            .layer(middleware::from_fn(print_request_response));

        Self { router, addr }
    }

    pub fn with_ui(self, path: PathBuf) -> Self {
        let error_handler = |error: io::Error| async move {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Unhandled internal error: {}", error),
            )
        };

        let mut router = self.router;
        info!("attaching ui static files handler...");
        let index =
            get_service(ServeFile::new(path.join("index.html"))).handle_error(error_handler);
        let favicon =
            get_service(ServeFile::new(path.join("favicon.ico"))).handle_error(error_handler);
        let assets = get_service(ServeDir::new(path.join("assets"))).handle_error(error_handler);
        // TODO resolve actual routes and distinguish them from 404s
        router = router.fallback(index);
        router = router.route("/favicon.ico", favicon);
        router = router.nest("/assets", assets);

        Self {
            router,
            addr: self.addr,
        }
    }
    pub async fn serve(self) -> Result<()> {
        let server = Server::bind(&self.addr).serve(self.router.into_make_service());
        let graceful = server.with_graceful_shutdown(async {
            let mut sig_int = tokio::signal::unix::signal(SignalKind::interrupt())
                .expect("failed to install signal");
            let mut sig_term = tokio::signal::unix::signal(SignalKind::terminate())
                .expect("failed to install signal");
            select! {
                _=sig_int.recv()=>info!("SIGINT received"),
                _=sig_term.recv()=>info!("SIGTERM received"),
            }
        });

        Ok(graceful.await?)
    }
}
