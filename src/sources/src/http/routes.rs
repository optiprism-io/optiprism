//! Axum routes setup

use std::sync::Arc;

use axum::extract::Path;
use axum::response::IntoResponse;
use axum::response::Response;
use axum::routing;
use axum::Extension;
use axum::Json;
use axum::Router;
use hyper::StatusCode;
use ingester::error::Error as IngesterError;
use ingester::pipeline::Ingester;

use super::entities::TrackRequest;
use super::entities::TrackResponse;
use super::ProjectTrackRequest;

struct Error(IngesterError);

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
    Extension(ingester): Extension<Arc<Ingester>>,
    Path((organization_id, project_id)): Path<(u64, u64)>,
    Json(request): Json<TrackRequest>,
) -> Result<(StatusCode, Json<TrackResponse>), Error> {
    let project_track_request = ProjectTrackRequest {
        organization_id,
        project_id,
        request,
    };
    let event_record_id = ingester
        .ingest(project_track_request)
        .await
        .map_err(Error)?;
    Ok((StatusCode::CREATED, Json(event_record_id.into())))
}

/// Attach ingester routes to the provided router.
pub fn attach_routes(router: Router) -> Router {
    router.nest(
        "/organizations/:organization_id/projects/:project_id/track/events",
        Router::new().route("/", routing::post(create)),
    )
}
