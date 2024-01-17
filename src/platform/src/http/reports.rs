use std::sync::Arc;

use axum::extract::Extension;
use axum::extract::Path;
use axum::http::StatusCode;
use axum::routing;
use axum::Router;
use common::http::Json;

use crate::reports::CreateReportRequest;
use crate::reports::Report;
use crate::reports::Reports;
use crate::reports::UpdateReportRequest;
use crate::Context;
use crate::ListResponse;
use crate::Result;

async fn create(
    ctx: Context,
    Extension(provider): Extension<Arc<Reports>>,
    Path(project_id): Path<u64>,
    Json(request): Json<CreateReportRequest>,
) -> Result<(StatusCode, Json<Report>)> {
    Ok((
        StatusCode::CREATED,
        Json(provider.create(ctx, project_id, request).await?),
    ))
}

async fn get_by_id(
    ctx: Context,
    Extension(provider): Extension<Arc<Reports>>,
    Path((project_id, report_id)): Path<(u64, u64)>,
) -> Result<Json<Report>> {
    Ok(Json(provider.get_by_id(ctx, project_id, report_id).await?))
}

async fn list(
    ctx: Context,
    Extension(provider): Extension<Arc<Reports>>,
    Path(project_id): Path<u64>,
) -> Result<Json<ListResponse<Report>>> {
    Ok(Json(provider.list(ctx, project_id).await?))
}

async fn update(
    ctx: Context,
    Extension(provider): Extension<Arc<Reports>>,
    Path((project_id, report_id)): Path<(u64, u64)>,
    Json(request): Json<UpdateReportRequest>,
) -> Result<Json<Report>> {
    Ok(Json(
        provider.update(ctx, project_id, report_id, request).await?,
    ))
}

async fn delete(
    ctx: Context,
    Extension(provider): Extension<Arc<Reports>>,
    Path((project_id, report_id)): Path<(u64, u64)>,
) -> Result<Json<Report>> {
    Ok(Json(provider.delete(ctx, project_id, report_id).await?))
}

pub fn attach_routes(router: Router) -> Router {
    router.nest(
        "/organizations/*/projects/:project_id/reports",
        Router::new()
            .route("/", routing::post(create).get(list))
            .route(
                "/:project_id",
                routing::get(get_by_id).delete(delete).put(update),
            ),
    )
}
