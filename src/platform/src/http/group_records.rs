use std::sync::Arc;

use axum::extract::Extension;
use axum::extract::Path;
use axum::routing;
use axum::Router;

use crate::group_records;
use crate::group_records::GroupRecord;
use crate::group_records::ListGroupRecordsRequest;
use crate::group_records::UpdateGroupRecordRequest;
use crate::http::Json;
use crate::Context;
use crate::ListResponse;
use crate::Result;

async fn get_by_id(
    ctx: Context,
    Extension(provider): Extension<Arc<dyn group_records::Provider>>,
    Path((organization_id, project_id, id)): Path<(u64, u64, u64)>,
) -> Result<Json<GroupRecord>> {
    Ok(Json(
        provider
            .get_by_id(ctx, organization_id, project_id, id)
            .await?,
    ))
}

async fn list(
    ctx: Context,
    Extension(provider): Extension<Arc<dyn group_records::Provider>>,
    Path((organization_id, project_id)): Path<(u64, u64)>,
    Json(request): Json<ListGroupRecordsRequest>,
) -> Result<Json<ListResponse<GroupRecord>>> {
    Ok(Json(
        provider
            .list(ctx, organization_id, project_id, request)
            .await?,
    ))
}

async fn update(
    ctx: Context,
    Extension(provider): Extension<Arc<dyn group_records::Provider>>,
    Path((organization_id, project_id, id)): Path<(u64, u64, u64)>,
    Json(request): Json<UpdateGroupRecordRequest>,
) -> Result<Json<GroupRecord>> {
    Ok(Json(
        provider
            .update(ctx, organization_id, project_id, id, request)
            .await?,
    ))
}

pub fn attach_routes(router: Router) -> Router {
    router.nest(
        "/organizations/:organization_id/projects/:project_id/group-records",
        Router::new()
            .route("/search", routing::post(list))
            .route("/:id", routing::get(get_by_id).put(update)),
    )
}
