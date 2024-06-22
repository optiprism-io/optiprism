use std::sync::Arc;

use axum::extract::Extension;
use axum::extract::Path;
use axum::extract::Query;
use axum::routing;
use axum::Router;
use common::http::Json;
use serde_json::Value;

use crate::{Context, QueryParams};
use crate::event_segmentation::{EventSegmentation, EventSegmentationRequest};
use crate::funnel::{Funnel, FunnelRequest};
use crate::FunnelResponse;
use crate::ListResponse;
use crate::QueryResponse;
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
        Router::new()
            .route(
                "/queries/funnel",
                routing::post(funnel),
            )
    )
}
