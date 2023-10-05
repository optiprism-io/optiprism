//! Ingester HTTP endpoints.

use std::sync::Arc;

use axum::extract::Path;
use axum::routing;
use axum::Extension;
use axum::Json;
use axum::Router;
use hyper::StatusCode;
use metadata::events::Provider as EventsProvider;
use metadata::properties::Provider as PropertiesProvider;
use serde::Serialize;

use crate::ingester::TrackRequest;

async fn create(
    Extension(event_provider): Extension<Arc<dyn EventsProvider>>,
    Extension(properties_provider): Extension<Arc<dyn PropertiesProvider>>,
    Path((organization_id, project_id)): Path<(u64, u64)>,
    Json(request): Json<TrackRequest>,
) -> Result<(StatusCode, Json<TrackResponse>), StatusCode> {
    Ok((StatusCode::CREATED, Json(0.into())))
}

#[derive(Debug, Serialize)]
struct TrackResponse {
    id: u64,
}

impl From<u64> for TrackResponse {
    fn from(id: u64) -> Self {
        TrackResponse { id }
    }
}

/// Attach ingester routes to the provided router.
pub fn attach_routes(router: Router) -> Router {
    router.nest(
        "/organizations/:organization_id/projects/:project_id/track/events",
        Router::new().route("/", routing::post(create)),
    )
}
