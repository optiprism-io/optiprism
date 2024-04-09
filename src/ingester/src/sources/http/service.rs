use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex;

use axum::extract::ConnectInfo;
use axum::extract::Path;
use axum::http::StatusCode;
use axum::routing;
use axum::Extension;
use axum::Router;
use axum_macros::debug_handler;
use chrono::Utc;
use common::http::Json;
use common::types::EVENT_CLICK;
use common::types::EVENT_PAGE;
use common::types::EVENT_SCREEN;
use tower::ServiceBuilder;
use tower_http::cors::Any;
use tower_http::cors::CorsLayer;
use tracing::info;

use crate::error::Result;
use crate::executor::Executor;
use crate::sources::http::IdentifyRequest;
use crate::sources::http::TrackRequest;
use crate::Context;
use crate::RequestContext;
