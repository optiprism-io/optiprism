use std::sync::Arc;

use axum::extract::Extension;
use axum::extract::Path;
use axum::http::StatusCode;
use axum::routing;
use axum::Router;
use common::http::Json;

use crate::dashboards::CreateDashboardRequest;
use crate::dashboards::Dashboard;
use crate::dashboards::Dashboards;
use crate::dashboards::UpdateDashboardRequest;
use crate::Context;
use crate::ListResponse;
use crate::Result;

async fn create(
    ctx: Context,
    Extension(provider): Extension<Arc<Dashboards>>,
    Path(project_id): Path<u64>,
    Json(request): Json<CreateDashboardRequest>,
) -> Result<(StatusCode, Json<Dashboard>)> {
    Ok((
        StatusCode::CREATED,
        Json(provider.create(ctx, project_id, request).await?),
    ))
}

async fn get_by_id(
    ctx: Context,
    Extension(provider): Extension<Arc<Dashboards>>,
    Path((project_id, dashboard_id)): Path<(u64, u64)>,
) -> Result<Json<Dashboard>> {
    Ok(Json(
        provider.get_by_id(ctx, project_id, dashboard_id).await?,
    ))
}

async fn list(
    ctx: Context,
    Extension(provider): Extension<Arc<Dashboards>>,
    Path(project_id): Path<u64>,
) -> Result<Json<ListResponse<Dashboard>>> {
    Ok(Json(provider.list(ctx, project_id).await?))
}

async fn update(
    ctx: Context,
    Extension(provider): Extension<Arc<Dashboards>>,
    Path((project_id, dashboard_id)): Path<(u64, u64)>,
    Json(request): Json<UpdateDashboardRequest>,
) -> Result<Json<Dashboard>> {
    Ok(Json(
        provider
            .update(ctx, project_id, dashboard_id, request)
            .await?,
    ))
}

async fn delete(
    ctx: Context,
    Extension(provider): Extension<Arc<Dashboards>>,
    Path((project_id, dashboard_id)): Path<(u64, u64)>,
) -> Result<Json<Dashboard>> {
    Ok(Json(provider.delete(ctx, project_id, dashboard_id).await?))
}

pub fn attach_routes(router: Router) -> Router {
    router.nest(
        "/organizations/*/projects/:project_id/dashboards",
        Router::new()
            .route("/", routing::post(create).get(list))
            .route(
                "/:dashboard_id",
                routing::get(get_by_id).delete(delete).put(update),
            ),
    )
}
