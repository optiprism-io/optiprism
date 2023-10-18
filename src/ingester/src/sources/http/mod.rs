use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex;

use axum::extract::Path;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing;
use axum::Json;
use axum::Router;
use axum::Server;
use tokio::select;
use tokio::signal::unix::SignalKind;
use tower_http::cors::Any;
use tower_http::cors::CorsLayer;
use tracing::info;

use crate::error::Result;
use crate::executor::Executor;
use crate::sink::Sink;
use crate::sources::http::track::TrackRequest;
use crate::sources::http::track::TrackResponse;

mod track;

#[derive(Clone)]
struct App {
    executor: Mutex<Executor>,
}

impl App {
    pub fn track(&self, token: &str, req: TrackRequest) -> Result<()> {
        let context = req.context.map(|ctx| crate::track::Context {
            library: ctx.library.map(|lib| crate::track::Library {
                name: lib.name.clone(),
                version: lib.version.clone(),
            }),
            page: ctx.page.map(|page| crate::track::Page {
                path: page.path.clone(),
                referrer: page.referrer.clone(),
                search: page.search.clone(),
                title: page.title.clone(),
                url: page.url.clone(),
            }),
            user_agent: ctx.user_agent.clone(),
            ip: ctx.ip.clone(),
        });

        let raw_properties = req
            .properties
            .iter()
            .map(|(k, v)| (k.to_owned(), v.to_owned()))
            .collect::<_>();
        let raw_user_properties = req
            .user_properties
            .iter()
            .map(|(k, v)| (k.to_owned(), v.to_owned()))
            .collect::<_>();
        let track = crate::track::Track {
            user_id: req.user_id.clone(),
            sent_at: req.sent_at.clone(),
            context,
            event: "".to_string(),
            raw_properties,
            raw_user_properties,
            properties: None,
            user_properties: None,
        };

        self.executor.lock().unwrap().execute(token, track)
    }
}

pub struct Service {
    router: Router,
    addr: SocketAddr,
}

impl Service {
    pub fn new(executor: Executor, addr: SocketAddr) -> Self {
        let mut router = Router::new();

        let state = App {
            executor: Mutex::new(executor),
        };
        info!("attaching api routes...");
        let cors = CorsLayer::new()
            .allow_methods(Any)
            .allow_origin(Any)
            .allow_headers(Any);

        let router = router
            .route("/v1/ingest/:token/track", routing::post(track))
            .layer(cors)
            .with_state(state);

        Self { router, addr }
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

async fn track(
    State(state): State(App),
    Path(token): Path<&str>,
    Json(request): Json<TrackRequest>,
) -> Result<(StatusCode, Json<TrackResponse>)> {
    Ok((
        StatusCode::CREATED,
        Json(state.0.track(token, request).await?),
    ))
}
