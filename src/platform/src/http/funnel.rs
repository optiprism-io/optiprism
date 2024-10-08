use std::sync::Arc;

use axum::extract::Extension;
use axum::extract::Path;
use axum::extract::Query;
use axum::routing;
use axum::Router;
use common::http::Json;

use crate::funnel::Funnel;
use crate::funnel::FunnelRequest;
use crate::Context;
use crate::FunnelResponse;
use crate::QueryParams;
use crate::Result;

async fn funnel(
    ctx: Context,
    Extension(provider): Extension<Arc<Funnel>>,
    Path(project_id): Path<u64>,
    Query(query): Query<QueryParams>,
    Json(request): Json<FunnelRequest>,
) -> Result<Json<FunnelResponse>> {
    Ok(Json(
        provider.funnel(ctx, project_id, request, query).await?,
    ))
}

pub fn attach_routes(router: Router) -> Router {
    router.nest(
        "/api/v1/projects/:project_id",
        Router::new().route("/queries/funnel", routing::post(funnel)),
    )
}
