use std::sync::Arc;

use axum::extract::{Extension, Query};
use axum::extract::Path;
use axum::routing;
use axum::Router;
use common::http::Json;

use crate::{QueryParams, QueryResponse};
use crate::group_records::{GroupRecord, GroupRecords, GroupRecordsSearchRequest};
use crate::Context;
use crate::Result;

async fn get_by_id(
    ctx: Context,
    Extension(provider): Extension<Arc<GroupRecords>>,
    Path((project_id, group_id, id)): Path<(u64, usize, String)>,
) -> Result<Json<GroupRecord>> {
    Ok(Json(provider.get_by_id(ctx, project_id, group_id, id).await?))
}

async fn search(
    ctx: Context,
    Extension(provider): Extension<Arc<GroupRecords>>,
    Path(project_id): Path<u64>,
    Query(query): Query<QueryParams>,
    Json(request): Json<GroupRecordsSearchRequest>,
) -> Result<Json<QueryResponse>> {
    Ok(Json(
        provider
            .search(ctx, project_id, request, query)
            .await?,
    ))
}



pub fn attach_routes(router: Router) -> Router {
    router.nest(
        "/api/v1/projects/:project_id/group-records",
        Router::new()
            .route("/search", routing::post(search))
            .route("/:group_id/:id", routing::get(get_by_id)),
    )
}
