//! Ingester HTTP endpoints.

use std::sync::Arc;

use axum::extract::Path;
use axum::routing;
use axum::Extension;
use axum::Json;
use axum::Router;
use hyper::StatusCode;
use serde::Serialize;

use crate::ingester::TrackRequest;

async fn create(
    // Extension(event_kind_provider): Extension<Arc<dyn ...>>, // TODO(metadata) provide kinds of events (user searched/user clicked/etc)
    // Extension(event_property_provider): Extension<Arc<dyn ...>>, // TODO(metadata) provide event properties (shared fields)
    // Extension(event_provider): Extension<Arc<dyn ...>>, // TODO provider to perform actual events persistence
    Path((organization_id, project_id, id)): Path<(u64, u64, u64)>,
    Json(request): Json<TrackRequest>,
) -> Result<(StatusCode, Json<TrackResponse>), StatusCode> {
    todo!()
}

#[derive(Debug, Serialize)]
struct TrackResponse {
    id: usize,
}

pub(super) fn attach_routes(router: Router) -> Router {
    router.nest(
        "/organizations/:organization_id/projects/:project_id/track/events",
        Router::new().route("/", routing::post(create)),
    )
}
