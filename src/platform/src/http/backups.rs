use std::sync::Arc;

use axum::extract::Extension;
use axum::extract::Path;
use axum::http::StatusCode;
use axum::routing;
use axum::Router;
use common::http::Json;
use crate::backups::{Backup, Backups};
use crate::bookmarks::{Bookmark, Bookmarks, CreateBookmarkRequest};
use crate::reports::CreateReportRequest;
use crate::reports::Report;
use crate::reports::Reports;
use crate::reports::UpdateReportRequest;
use crate::Context;
use crate::ListResponse;
use crate::Result;

async fn backup(
    ctx: Context,
    Extension(provider): Extension<Arc<Backups>>,
) -> Result<(StatusCode, Json<Backup>)> {
    Ok((
        StatusCode::NO_CONTENT,
        Json(provider.backup(ctx).await?),
    ))
}

async fn get_by_id(
    ctx: Context,
    Extension(provider): Extension<Arc<Backups>>,
    Path(backup_id): Path<u64>,
) -> Result<Json<Backup>> {
    Ok(Json(provider.get_by_id(ctx, backup_id).await?))
}

async fn list(
    ctx: Context,
    Extension(provider): Extension<Arc<Backups>>,
) -> Result<Json<ListResponse<Backup>>> {
    Ok(Json(provider.list(ctx).await?))
}

pub fn attach_routes(router: Router) -> Router {
    router.nest(
        "/api/v1/admin/backups",
        Router::new()
            .route("/", routing::get(list))
            .route("/backup", routing::post(backup))
            .route(
                "/:backup_id",
                routing::get(get_by_id),
            ),
    )
}
