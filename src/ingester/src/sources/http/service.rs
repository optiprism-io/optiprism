use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex;

use axum::extract::Path;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing;
use axum::Router;
use axum::Server;
use axum_macros::debug_handler;
use common::error;
use common::http::Json;
use common::types::EVENT_CLICK;
use common::types::EVENT_PAGE;
use common::types::EVENT_SCREEN;
use tokio::select;
use tokio::signal::unix::SignalKind;
use tower_http::cors::Any;
use tower_http::cors::CorsLayer;
use tracing::info;

use crate::error::Result;
use crate::executor::Executor;
use crate::sources::http::IdentifyRequest;
use crate::sources::http::TrackRequest;
use crate::Context;

#[debug_handler]
async fn track(
    State(state): State<App>,
    Path(token): Path<String>,
    Json(request): Json<TrackRequest>,
) -> Result<StatusCode> {
    state.track(token, request)?;
    Ok(StatusCode::CREATED)
}

#[debug_handler]
async fn click(
    State(state): State<App>,
    Path(token): Path<String>,
    Json(request): Json<TrackRequest>,
) -> Result<StatusCode> {
    state.click(token, request)?;
    Ok(StatusCode::CREATED)
}

#[debug_handler]
async fn page(
    State(state): State<App>,
    Path(token): Path<String>,
    Json(request): Json<TrackRequest>,
) -> Result<StatusCode> {
    state.page(token, request)?;
    Ok(StatusCode::CREATED)
}

#[debug_handler]
async fn screen(
    State(state): State<App>,
    Path(token): Path<String>,
    Json(request): Json<TrackRequest>,
) -> Result<StatusCode> {
    state.screen(token, request)?;
    Ok(StatusCode::CREATED)
}

#[debug_handler]
async fn identify(
    State(state): State<App>,
    Path(token): Path<String>,
    Json(request): Json<IdentifyRequest>,
) -> Result<StatusCode> {
    state.identify(token, request)?;
    Ok(StatusCode::CREATED)
}

#[derive(Clone)]
struct App {
    track: Arc<Mutex<Executor<crate::Track>>>,
    identify: Arc<Mutex<Executor<crate::Identify>>>,
}

impl App {
    pub fn track(&self, token: String, req: TrackRequest) -> Result<()> {
        let context = Context {
            library: req.context.library.map(|lib| crate::Library {
                name: lib.name.clone(),
                version: lib.version.clone(),
            }),
            page: req.context.page.map(|page| crate::Page {
                path: page.path.clone(),
                referrer: page.referrer.clone(),
                search: page.search.clone(),
                title: page.title.clone(),
                url: page.url.clone(),
            }),
            user_agent: req.context.user_agent.clone(),
            ip: req.context.ip.clone(),
        };

        let raw_properties = req.properties.map(|v| {
            v.into_iter()
                .map(|(k, v)| (k.to_owned(), v.into()))
                .collect::<_>()
        });
        let raw_user_properties = req.user_properties.map(|v| {
            v.into_iter()
                .map(|(k, v)| (k.to_owned(), v.into()))
                .collect::<_>()
        });
        let track = crate::Track {
            user_id: req.user_id.clone(),
            anonymous_id: req.anonymous_id.clone(),
            resolved_user_id: None,
            resolved_anonymous_user_id: None,
            sent_at: req.sent_at.clone(),
            context,
            event: req.event.clone().unwrap(),
            resolved_event: None,
            properties: raw_properties,
            user_properties: raw_user_properties,
            resolved_properties: None,
            resolved_user_properties: None,
        };

        self.track.lock().unwrap().execute(token, track)
    }

    pub fn click(&self, token: String, mut req: TrackRequest) -> Result<()> {
        req.event = Some(EVENT_CLICK.to_string());
        self.track(token, req)
    }

    pub fn page(&self, token: String, mut req: TrackRequest) -> Result<()> {
        req.event = Some(EVENT_PAGE.to_string());
        self.track(token, req)
    }

    pub fn screen(&self, token: String, mut req: TrackRequest) -> Result<()> {
        req.event = Some(EVENT_SCREEN.to_string());
        self.track(token, req)
    }

    pub fn identify(&self, token: String, mut req: IdentifyRequest) -> Result<()> {
        let context = Context {
            library: req.context.library.map(|lib| crate::Library {
                name: lib.name.clone(),
                version: lib.version.clone(),
            }),
            page: req.context.page.map(|page| crate::Page {
                path: page.path.clone(),
                referrer: page.referrer.clone(),
                search: page.search.clone(),
                title: page.title.clone(),
                url: page.url.clone(),
            }),
            user_agent: req.context.user_agent.clone(),
            ip: req.context.ip.clone(),
        };

        let raw_user_properties = req.user_properties.map(|v| {
            v.into_iter()
                .map(|(k, v)| (k.to_owned(), v.into()))
                .collect::<_>()
        });
        let track = crate::Identify {
            user_id: req.user_id.clone(),
            resolved_user_id: None,
            sent_at: req.sent_at.clone(),
            context,
            event: req.event.clone().unwrap(),
            user_properties: raw_user_properties,
            resolved_user_properties: None,
        };

        self.identify.lock().unwrap().execute(token, track)
    }
}

pub struct Service {
    router: Router,
    addr: SocketAddr,
}

impl Service {
    pub fn new(
        track_exec: Executor<crate::Track>,
        identify_exec: Executor<crate::Identify>,
        addr: SocketAddr,
    ) -> Self {
        let mut router = Router::new();

        let state = App {
            track: Arc::new(Mutex::new(track_exec)),
            identify: Arc::new(Mutex::new((identify_exec))),
        };
        info!("attaching api routes...");
        let cors = CorsLayer::new()
            .allow_methods(Any)
            .allow_origin(Any)
            .allow_headers(Any);

        let router = router
            .route("/v1/ingest/:token/track", routing::post(track))
            .route("/v1/ingest/:token/click", routing::post(click))
            .route("/v1/ingest/:token/page", routing::post(page))
            .route("/v1/ingest/:token/screen", routing::post(screen))
            .route("/v1/ingest/:token/identify", routing::post(identify))
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
