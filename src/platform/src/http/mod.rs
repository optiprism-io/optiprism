pub mod accounts;
pub mod auth;
pub mod custom_events;
pub mod events;
pub mod properties;
pub mod queries;

use std::collections::BTreeMap;
use std::error::Error;
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use axum::async_trait;
use axum::body::HttpBody;
use axum::extract::rejection::JsonRejection;
use axum::http::Request;
use axum::http::StatusCode;
use axum::middleware;
use axum::middleware::Next;
use axum::routing::get_service;
use axum::Extension;
use axum::Router;
use axum::Server;
use axum_core::extract::FromRequest;
use axum_core::extract::RequestParts;
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
use tokio::select;
use tokio::signal::unix::SignalKind;
use tower_cookies::CookieManagerLayer;
use tower_http::services::ServeDir;
use tower_http::services::ServeFile;
use tower_http::trace::TraceLayer;
use tracing::info;

use crate::error::ApiError;
use crate::PlatformProvider;
use crate::Result;

pub struct Service {
    router: Router,
    addr: SocketAddr,
}

impl Service {
    pub fn new(
        md: &Arc<MetadataProvider>,
        platform: &Arc<PlatformProvider>,
        auth_cfg: crate::auth::Config,
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
            .layer(Extension(auth_cfg))
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

pub async fn print_request_response(
    req: Request<Body>,
    next: Next<Body>,
) -> std::result::Result<impl IntoResponse, (StatusCode, String)> {
    let (parts, body) = req.into_parts();
    tracing::debug!("headers = {:?}", parts.headers);
    let bytes = buffer_and_print("request", body).await?;
    let req = Request::from_parts(parts, Body::from(bytes));

    let res = next.run(req).await;

    let (parts, body) = res.into_parts();
    let bytes = buffer_and_print("response", body).await?;
    let res = Response::from_parts(parts, Body::from(bytes));

    Ok(res)
}

async fn buffer_and_print<B>(
    direction: &str,
    body: B,
) -> std::result::Result<Bytes, (StatusCode, String)>
where
    B: axum::body::HttpBody<Data = Bytes>,
    B::Error: std::fmt::Display,
{
    let bytes = match hyper::body::to_bytes(body).await {
        Ok(bytes) => bytes,
        Err(err) => {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("failed to read {} body: {}", direction, err),
            ));
        }
    };

    if let Ok(body) = std::str::from_utf8(&bytes) {
        tracing::debug!("{} body = {:?}", direction, body);
    }

    Ok(bytes)
}

#[derive(Debug, Clone, Copy, Default)]
pub struct Json<T>(pub T);

#[async_trait]
impl<T, B> FromRequest<B> for Json<T>
where
    T: DeserializeOwned,
    B: HttpBody + Send,
    B::Data: Send,
    B::Error: Into<BoxError>,
{
    type Rejection = ApiError;

    async fn from_request(req: &mut RequestParts<B>) -> std::result::Result<Self, Self::Rejection> {
        match axum::Json::<T>::from_request(req).await {
            Ok(v) => Ok(Json(v.0)),
            Err(err) => {
                let src_err = err.source().unwrap().source().unwrap();
                let mut api_err = ApiError::bad_request(format!("{err}: {src_err}"));
                // add field information
                match &err {
                    JsonRejection::JsonDataError(_) => {
                        lazy_static! {
                            static ref FIELD_RX: Regex =
                                Regex::new(r"(\w+?) field `(.+?)`").unwrap();
                        }
                        if let Some(captures) = FIELD_RX.captures(src_err.to_string().as_str()) {
                            api_err = api_err.with_fields(BTreeMap::from([(
                                captures[2].to_string(),
                                captures[1].to_string(),
                            )]));
                        }
                    }
                    JsonRejection::JsonSyntaxError(_) => {}
                    JsonRejection::MissingJsonContentType(_) => {}
                    JsonRejection::BytesRejection(_) => {}
                    _ => panic!(),
                }

                Err(api_err)
            }
        }
    }
}

impl<T> IntoResponse for Json<T>
where T: Serialize
{
    fn into_response(self) -> Response {
        axum::Json(self.0).into_response()
    }
}
