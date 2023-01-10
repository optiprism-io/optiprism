use std::sync::Arc;

use axum::extract::Extension;
use axum::extract::Path;
use axum::routing;
use axum::Router;

use crate::event_records;
use crate::event_records::EventRecord;
use crate::event_records::ListEventRecordsRequest;
use crate::http::Json;
use crate::Context;
use crate::ListResponse;
use crate::Result;

async fn list(
    ctx: Context,
    Extension(provider): Extension<Arc<dyn event_records::Provider>>,
    Path((organization_id, project_id)): Path<(u64, u64)>,
    Json(request): Json<ListEventRecordsRequest>,
) -> Result<Json<ListResponse<EventRecord>>> {
    Ok(Json(
        provider
            .list(ctx, organization_id, project_id, request)
            .await?,
    ))
}

async fn get_by_id(
    ctx: Context,
    Extension(provider): Extension<Arc<dyn event_records::Provider>>,
    Path((organization_id, project_id, id)): Path<(u64, u64, u64)>,
) -> Result<Json<EventRecord>> {
    Ok(Json(
        provider
            .get_by_id(ctx, organization_id, project_id, id)
            .await?,
    ))
}

pub fn attach_routes(router: Router) -> Router {
    router.nest(
        "/organizations/:organization_id/projects/:project_id/event-records",
        Router::new()
            .route("/search", routing::post(list))
            .route("/:id", routing::get(get_by_id)),
    )
}