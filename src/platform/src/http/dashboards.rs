use std::sync::Arc;

use axum::extract::Extension;
use axum::extract::Path;
use axum::http::StatusCode;
use axum::routing;
use axum::Router;

use crate::{dashboards};
use crate::http::Json;
use crate::Context;
use crate::dashboards::{CreateDashboardRequest, Dashboard, UpdateDashboardRequest};
use crate::ListResponse;
use crate::Result;

async fn create(
    ctx: Context,
    Extension(provider): Extension<Arc<dyn dashboards::Provider>>,
    Path((organization_id, project_id)): Path<(u64, u64)>,
    Json(request): Json<CreateDashboardRequest>,
) -> Result<(StatusCode, Json<Dashboard>)> {
    Ok((
        StatusCode::CREATED,
        Json(
            provider
                .create(ctx, organization_id, project_id, request)
                .await?,
        ),
    ))
}

async fn get_by_id(
    ctx: Context,
    Extension(provider): Extension<Arc<dyn dashboards::Provider>>,
    Path((organization_id, project_id, dashboard_id)): Path<(u64, u64, u64)>,
) -> Result<Json<Dashboard>> {
    Ok(Json(
        provider
            .get_by_id(ctx, organization_id, project_id, dashboard_id)
            .await?,
    ))
}

async fn list(
    ctx: Context,
    Extension(provider): Extension<Arc<dyn dashboards::Provider>>,
    Path((organization_id, project_id)): Path<(u64, u64)>,
) -> Result<Json<ListResponse<Dashboard>>> {
    Ok(Json(provider.list(ctx, organization_id, project_id).await?))
}

async fn update(
    ctx: Context,
    Extension(provider): Extension<Arc<dyn dashboards::Provider>>,
    Path((organization_id, project_id, dashboard_id)): Path<(u64, u64, u64)>,
    Json(request): Json<UpdateDashboardRequest>,
) -> Result<Json<Dashboard>> {
    Ok(Json(
        provider
            .update(ctx, organization_id, project_id, dashboard_id, request)
            .await?,
    ))
}

async fn delete(
    ctx: Context,
    Extension(provider): Extension<Arc<dyn dashboards::Provider>>,
    Path((organization_id, project_id, dashboard_id)): Path<(u64, u64, u64)>,
) -> Result<Json<Dashboard>> {
    Ok(Json(
        provider
            .delete(ctx, organization_id, project_id, dashboard_id)
            .await?,
    ))
}

pub fn attach_routes(router: Router) -> Router {
    router.clone().nest(
        "/organizations/:organization_id/projects/:project_id/dashboards",
        router
            .route("/", routing::post(create).get(list))
            .route(
                "/:project_id",
                routing::get(get_by_id).delete(delete).put(update),
            )
    )
}
