use std::sync::Arc;

use axum::extract::{Extension, Query};
use axum::extract::Path;
use axum::routing;
use axum::Router;
use common::http::Json;

use crate::{QueryParams, QueryResponse};
use crate::event_records::{EventRecord, EventRecords, EventRecordsSearchRequest};
use crate::Context;
use crate::Result;

async fn get_by_id(
    ctx: Context,
    Extension(provider): Extension<Arc<EventRecords>>,
    Path((project_id, id)): Path<(u64, u64)>,
) -> Result<Json<EventRecord>> {
    Ok(Json(provider.get_by_id(ctx, project_id, id).await?))
}

async fn search(
    ctx: Context,
    Extension(provider): Extension<Arc<EventRecords>>,
    Path(project_id): Path<u64>,
    Query(query): Query<QueryParams>,
    Json(request): Json<EventRecordsSearchRequest>,
) -> Result<Json<QueryResponse>> {
    Ok(Json(
        provider
            .search(ctx, project_id, request, query)
            .await?,
    ))
}


pub fn attach_routes(router: Router) -> Router {
    router.nest(
        "/api/v1/projects/:project_id/event-records",
        Router::new().route("/:id", routing::get(get_by_id))
            .route("/search", routing::post(search)),
    )
}
