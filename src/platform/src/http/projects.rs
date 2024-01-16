use std::sync::Arc;

use axum::extract::Extension;
use axum::extract::Path;
use axum::http::StatusCode;
use axum::routing;
use axum::Router;
use common::http::Json;

use crate::projects::CreateProjectRequest;
use crate::projects::Project;
use crate::projects::Projects;
use crate::projects::UpdateProjectRequest;
use crate::Context;
use crate::ListResponse;
use crate::Result;

async fn create(
    ctx: Context,
    Extension(provider): Extension<Arc<Projects>>,
    Path((organization_id, _project_id)): Path<(u64, u64)>,
    Json(request): Json<CreateProjectRequest>,
) -> Result<(StatusCode, Json<Project>)> {
    Ok((
        StatusCode::CREATED,
        Json(provider.create(ctx, organization_id, request).await?),
    ))
}

async fn get_by_id(
    ctx: Context,
    Extension(provider): Extension<Arc<Projects>>,
    Path((organization_id, project_id)): Path<(u64, u64)>,
) -> Result<Json<Project>> {
    Ok(Json(
        provider.get_by_id(ctx, organization_id, project_id).await?,
    ))
}

async fn list(
    ctx: Context,
    Extension(provider): Extension<Arc<Projects>>,
    Path(organization_id): Path<u64>,
) -> Result<Json<ListResponse<Project>>> {
    Ok(Json(provider.list(ctx, organization_id).await?))
}

async fn update(
    ctx: Context,
    Extension(provider): Extension<Arc<Projects>>,
    Path((organization_id, project_id)): Path<(u64, u64)>,
    Json(request): Json<UpdateProjectRequest>,
) -> Result<Json<Project>> {
    Ok(Json(
        provider
            .update(ctx, organization_id, project_id, request)
            .await?,
    ))
}

async fn delete(
    ctx: Context,
    Extension(provider): Extension<Arc<Projects>>,
    Path((organization_id, project_id)): Path<(u64, u64)>,
) -> Result<Json<Project>> {
    Ok(Json(
        provider.delete(ctx, organization_id, project_id).await?,
    ))
}

pub fn attach_routes(router: Router) -> Router {
    router.nest(
        "/organizations/:organization_id/projects",
        Router::new()
            .route("/", routing::post(create).get(list))
            .route(
                "/:project_id",
                routing::get(get_by_id).delete(delete).put(update),
            ),
    )
}
