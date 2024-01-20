use std::sync::Arc;

use axum::extract::Extension;
use axum::extract::Path;
use axum::routing;
use axum::Router;
use common::http::Json;

use crate::event_records;
use crate::event_records::EventRecord;
use crate::event_records::ListEventRecordsRequest;
use crate::Context;
use crate::ListResponse;
use crate::Result;

async fn list(
    ctx: Context,
    Extension(provider): Extension<Arc<dyn event_records::Provider>>,
    Path(project_id): Path<u64>,
    Json(request): Json<ListEventRecordsRequest>,
) -> Result<Json<ListResponse<EventRecord>>> {
    Ok(Json(provider.list(ctx, project_id, request).await?))
}

async fn get_by_id(
    ctx: Context,
    Extension(provider): Extension<Arc<dyn event_records::Provider>>,
    Path((project_id, id)): Path<(u64, u64)>,
) -> Result<Json<EventRecord>> {
    Ok(Json(provider.get_by_id(ctx, project_id, id).await?))
}

pub fn attach_routes(router: Router) -> Router {
    router.nest(
        "/projects/:project_id/event-records",
        Router::new().route("/:id", routing::get(get_by_id)),
    )
}
