use std::sync::Arc;

use axum::extract::Extension;
use axum::extract::Path;
use axum::http::StatusCode;
use axum::routing;
use axum::Router;
use common::http::Json;
use crate::bookmarks::{Bookmark, Bookmarks, CreateBookmarkRequest};
use crate::reports::CreateReportRequest;
use crate::reports::Report;
use crate::reports::Reports;
use crate::reports::UpdateReportRequest;
use crate::Context;
use crate::ListResponse;
use crate::Result;

async fn create(
    ctx: Context,
    Extension(provider): Extension<Arc<Bookmarks>>,
    Path(project_id): Path<u64>,
    Json(request): Json<CreateBookmarkRequest>,
) -> Result<(StatusCode, Json<Bookmark>)> {
    Ok((
        StatusCode::CREATED,
        Json(provider.create(ctx, project_id, request).await?),
    ))
}

async fn get_by_id(
    ctx: Context,
    Extension(provider): Extension<Arc<Bookmarks>>,
    Path((project_id, id)): Path<(u64, String)>,
) -> Result<Json<Bookmark>> {
    Ok(Json(provider.get(ctx, project_id, &id).await?))
}

pub fn attach_routes(router: Router) -> Router {
    router.nest(
        "/api/v1/projects/:project_id/bookmarks",
        Router::new()
            .route("/", routing::post(create))
            .route(
                "/:project_id",
                routing::get(get_by_id),
            ),
    )
}
