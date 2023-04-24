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
use axum_extra::routing::SpaRouter;
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
use tower_http::trace::TraceLayer;
use tracing::info;

use crate::error::ApiError;
use crate::PlatformError;
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
        router = dashboards::attach_routes(router);
        router = reports::attach_routes(router);
        router = event_records::attach_routes(router);
        router = group_records::attach_routes(router);
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

        Self { router, addr }
    }

    pub fn set_ui(self, maybe_path: Option<PathBuf>) -> Self {
        let path = if let Some(path) = maybe_path {
            path
        } else {
            return self;
        };

        //        let error_handler = |error: io::Error| async move {
        // (
        // StatusCode::INTERNAL_SERVER_ERROR,
        // format!("Unhandled internal error: {}", error),
        // )
        // };
        let mut router = self.router;
        info!("attaching ui static files handler...");
        router = router.merge(
            SpaRouter::new("/assets", path.join("assets")).index_file(path.join("index.html")),
        );

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

// async fn fallback(uri: Uri) -> impl IntoResponse {
//     PlatformError::NotFound(format!("No route for {}", uri))
// }

fn content_length(headers: &HeaderMap<HeaderValue>) -> Option<u64> {
    headers
        .get(http::header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok()?.parse::<u64>().ok())
}

pub async fn print_request_response(
    mut req: Request<Body>,
    next: Next<Body>,
) -> Result<impl IntoResponse> {
    tracing::debug!("request headers = {:?}", req.headers());

    if content_length(req.headers()).is_some() {
        let (parts, body) = req.into_parts();
        let bytes = buffer_and_print("request", body).await?;
        req = Request::from_parts(parts, Body::from(bytes));
    }

    let res = next.run(req).await;
    if content_length(res.headers()).is_none() {
        return Ok(res);
    }

    let (parts, body) = res.into_parts();
    let bytes = buffer_and_print("response", body).await?;

    Ok(Response::from_parts(parts, body::boxed(Body::from(bytes))))
}

async fn buffer_and_print<B>(direction: &str, body: B) -> Result<Bytes>
where
    B: HttpBody<Data = Bytes>,
    B::Error: std::fmt::Display,
{
    let bytes = match hyper::body::to_bytes(body).await {
        Ok(bytes) => bytes,
        Err(err) => {
            return Err(PlatformError::BadRequest(format!(
                "failed to read {direction} body: {err}"
            )));
        }
    };

    if let Ok(body) = std::str::from_utf8(&bytes) {
        let v = serde_json::from_slice::<Value>(body.as_bytes())?;
        tracing::debug!("{} body = {}", direction, serde_json::to_string_pretty(&v)?);
    }

    Ok(bytes)
}

#[derive(Debug, Clone, Copy, Default)]
pub struct Json<T>(pub T);

#[async_trait]
impl<T, S, B> FromRequest<S, B> for Json<T>
where
    T: DeserializeOwned,
    B: HttpBody + Send + 'static,
    B::Data: Send,
    B::Error: Into<BoxError>,
    S: Send + Sync,
{
    type Rejection = ApiError;

    async fn from_request(
        req: Request<B>,
        state: &S,
    ) -> std::result::Result<Self, Self::Rejection> {
        match axum::Json::<T>::from_request(req, state).await {
            Ok(v) => Ok(Json(v.0)),
            Err(err) => {
                let mut api_err = ApiError::bad_request(err.to_string());

                if let Some(inner) = err.source() {
                    if let Some(inner) = inner.source() {
                        api_err = api_err.append_inner_message(inner.to_string());
                        match err {
                            JsonRejection::JsonDataError(_) => {
                                lazy_static! {
                                    static ref FIELD_RX: Regex =
                                        Regex::new(r"(\w+?) field `(.+?)`").unwrap();
                                }
                                if let Some(captures) =
                                    FIELD_RX.captures(inner.to_string().as_str())
                                {
                                    api_err = api_err.with_fields(BTreeMap::from([(
                                        captures[2].to_string(),
                                        captures[1].to_string(),
                                    )]));
                                }
                            }
                            JsonRejection::JsonSyntaxError(_) => {}
                            JsonRejection::MissingJsonContentType(_) => {}
                            JsonRejection::BytesRejection(_) => {}
                            _ => unreachable!(),
                        }
                    }
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
