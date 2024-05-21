use std::sync::Arc;

use axum::extract::Extension;
use axum::extract::Path;
use axum::http::StatusCode;
use axum::routing;
use axum::Router;
use common::http::Json;

use crate::events::Event;
use crate::events::Events;
use crate::events::UpdateEventRequest;
use crate::groups::CreateGroupRequest;
use crate::groups::Group;
use crate::groups::Groups;
use crate::Context;
use crate::ListResponse;
use crate::Result;

async fn create(
    ctx: Context,
    Extension(provider): Extension<Arc<Groups>>,
    Path(project_id): Path<u64>,
    Json(request): Json<CreateGroupRequest>,
) -> Result<(StatusCode, Json<Group>)> {
    Ok((
        StatusCode::CREATED,
        Json(provider.create(ctx, project_id, request).await?),
    ))
}

async fn list(
    ctx: Context,
    Extension(provider): Extension<Arc<Groups>>,
    Path(project_id): Path<u64>,
) -> Result<Json<ListResponse<Group>>> {
    Ok(Json(provider.list(ctx, project_id).await?))
}

pub fn attach_routes(router: Router) -> Router {
    router.nest(
        "/api/v1/projects/:project_id/groups",
        Router::new().route("/", routing::post(create).get(list)),
    )
}
