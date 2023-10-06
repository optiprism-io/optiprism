//! Ingester HTTP endpoints.

use std::sync::Arc;

use axum::extract::Path;
use axum::response::IntoResponse;
use axum::response::Response;
use axum::routing;
use axum::Extension;
use axum::Json;
use axum::Router;
use hyper::StatusCode;
use metadata::atomic_counters::Provider as AtomicCountersProvider;
use metadata::error::MetadataError;
use metadata::events::Provider as EventsProvider;
use metadata::properties::Provider as PropertiesProvider;

use crate::ingester::TrackRequest;
use crate::ingester::TrackResponse;

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("metadata error: {0}")]
    Metadata(#[from] MetadataError),
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        // TODO: figure out whether ingester crate should even bother about responses for
        // errors, perhaps a middleware in platform should take care of it. This may
        // require complete error data from ingester to filter out details in platform for
        // security concerns.
        StatusCode::INTERNAL_SERVER_ERROR.into_response()
    }
}

async fn create(
    Extension(counters_provider): Extension<Arc<dyn AtomicCountersProvider>>,
    Extension(event_provider): Extension<Arc<dyn EventsProvider>>,
    Extension(properties_provider): Extension<Arc<dyn PropertiesProvider>>,
    Path((organization_id, project_id)): Path<(u64, u64)>,
    Json(request): Json<TrackRequest>,
) -> Result<(StatusCode, Json<TrackResponse>), Error> {
    let id = counters_provider
        .next_event_record(organization_id, project_id)
        .await?;

    Ok((StatusCode::CREATED, Json(id.into())))
}

/// Attach ingester routes to the provided router.
pub fn attach_routes(router: Router) -> Router {
    router.nest(
        "/organizations/:organization_id/projects/:project_id/track/events",
        Router::new().route("/", routing::post(create)),
    )
}
