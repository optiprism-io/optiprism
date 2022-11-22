use std::sync::Arc;

use axum::extract::Extension;
use axum::extract::Path;
use axum::http::StatusCode;
use axum::routing;
use axum::Router;

use crate::{reports, events};
use crate::http::Json;
use crate::Context;
use crate::reports::{CreateReportRequest, Report, UpdateReportRequest};
use crate::ListResponse;
use crate::Result;

async fn create(
    ctx: Context,
    Extension(provider): Extension<Arc<dyn reports::Provider>>,
    Path((organization_id, project_id)): Path<(u64, u64)>,
    Json(request): Json<CreateReportRequest>,
) -> Result<(StatusCode, Json<Report>)> {
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
    Extension(provider): Extension<Arc<dyn reports::Provider>>,
    Path((organization_id, project_id, report_id)): Path<(u64, u64, u64)>,
) -> Result<Json<Report>> {
    Ok(Json(
        provider
            .get_by_id(ctx, organization_id, project_id, report_id)
            .await?,
    ))
}

async fn list(
    ctx: Context,
    Extension(provider): Extension<Arc<dyn reports::Provider>>,
    Path((organization_id, project_id)): Path<(u64, u64)>,
) -> Result<Json<ListResponse<Report>>> {
    Ok(Json(provider.list(ctx, organization_id, project_id).await?))
}

async fn update(
    ctx: Context,
    Extension(provider): Extension<Arc<dyn reports::Provider>>,
    Path((organization_id, project_id, report_id)): Path<(u64, u64, u64)>,
    Json(request): Json<UpdateReportRequest>,
) -> Result<Json<Report>> {
    Ok(Json(
        provider
            .update(ctx, organization_id, project_id, report_id, request)
            .await?,
    ))
}

async fn delete(
    ctx: Context,
    Extension(provider): Extension<Arc<dyn reports::Provider>>,
    Path((organization_id, project_id, report_id)): Path<(u64, u64, u64)>,
) -> Result<Json<Report>> {
    Ok(Json(
        provider
            .delete(ctx, organization_id, project_id, report_id)
            .await?,
    ))
}

pub fn attach_routes(router: Router) -> Router {
    router.clone().nest(
        "/organizations/:organization_id/projects/:project_id/reports",
        router
            .route("/", routing::post(create).get(list))
            .route(
                "/:project_id",
                routing::get(get_by_id).delete(delete).put(update),
            )
    )
}
