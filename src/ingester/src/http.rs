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
use metadata::arrow::datatypes::DataType;
use metadata::atomic_counters::Provider as AtomicCountersProvider;
use metadata::events::CreateEventRequest;
use metadata::events::Provider as EventsProvider;
use metadata::properties::CreatePropertyRequest;
use metadata::properties::Provider as PropertiesProvider;

use crate::ingester::storage::create_event_metadata;
use crate::ingester::Error;
use crate::ingester::TrackRequest;
use crate::ingester::TrackResponse;
use crate::ingester::DEFAULT_USER_ID;

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
    Extension(events_provider): Extension<Arc<dyn EventsProvider>>,
    Extension(properties_provider): Extension<Arc<dyn PropertiesProvider>>,
    Path((organization_id, project_id)): Path<(u64, u64)>,
    Json(request): Json<TrackRequest>,
) -> Result<(StatusCode, Json<TrackResponse>), Error> {
    // Update event records' counter to get the next free id
    let id = counters_provider
        .next_event_record(organization_id, project_id)
        .await?;

    // Prepare event and properties metadata
    let event_metatada_id = create_event_metadata(
        events_provider.as_ref(),
        properties_provider.as_ref(),
        organization_id,
        project_id,
        request,
    )
    .await?;

    // TODO make an actual event record

    Ok((StatusCode::CREATED, Json(id.into())))
}

/// Attach ingester routes to the provided router.
pub fn attach_routes(router: Router) -> Router {
    router.nest(
        "/organizations/:organization_id/projects/:project_id/track/events",
        Router::new().route("/", routing::post(create)),
    )
}
