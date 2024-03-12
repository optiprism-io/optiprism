use std::sync::Arc;

use axum::extract::Extension;
use axum::extract::Path;
use axum::http::StatusCode;
use axum::routing;
use axum::Router;
use common::http::Json;

use crate::organizations::CreateOrganizationRequest;
use crate::organizations::Organization;
use crate::organizations::Organizations;
use crate::organizations::UpdateOrganizationRequest;
use crate::projects::CreateProjectRequest;
use crate::projects::Project;
use crate::projects::Projects;
use crate::projects::UpdateProjectRequest;
use crate::Context;
use crate::ListResponse;
use crate::Result;

async fn create(
    ctx: Context,
    Extension(provider): Extension<Arc<Organizations>>,
    Json(request): Json<CreateOrganizationRequest>,
) -> Result<(StatusCode, Json<Organization>)> {
    Ok((
        StatusCode::CREATED,
        Json(provider.create(ctx, request).await?),
    ))
}

async fn get_by_id(
    ctx: Context,
    Extension(provider): Extension<Arc<Organizations>>,
    Path(org_id): Path<u64>,
) -> Result<Json<Organization>> {
    Ok(Json(provider.get_by_id(ctx, org_id).await?))
}

async fn list(
    ctx: Context,
    Extension(provider): Extension<Arc<Organizations>>,
) -> Result<Json<ListResponse<Organization>>> {
    Ok(Json(provider.list(ctx).await?))
}

async fn update(
    ctx: Context,
    Extension(provider): Extension<Arc<Organizations>>,
    Path(org_id): Path<u64>,
    Json(request): Json<UpdateOrganizationRequest>,
) -> Result<Json<Organization>> {
    Ok(Json(provider.update(ctx, org_id, request).await?))
}

async fn delete(
    ctx: Context,
    Extension(provider): Extension<Arc<Organizations>>,
    Path(org_id): Path<u64>,
) -> Result<Json<Organization>> {
    Ok(Json(provider.delete(ctx, org_id).await?))
}

pub fn attach_routes(router: Router) -> Router {
    router.nest(
        "/api/v1/organizations",
        Router::new()
            .route("/", routing::post(create).get(list))
            .route(
                "/:org_id",
                routing::get(get_by_id).delete(delete).put(update),
            ),
    )
}
