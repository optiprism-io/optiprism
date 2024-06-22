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
use crate::FunnelResponse;
use crate::ListResponse;
use crate::QueryResponse;
use crate::Result;

async fn event_segmentation(
    ctx: Context,
    Extension(provider): Extension<Arc<EventSegmentation>>,
    Path(project_id): Path<u64>,
    Query(query): Query<QueryParams>,
    Json(request): Json<EventSegmentationRequest>,
) -> Result<Json<QueryResponse>> {
    Ok(Json(
        provider
            .event_segmentation(ctx, project_id, request, query)
            .await?,
    ))
}

pub fn attach_routes(router: Router) -> Router {
    router.nest(
        "/api/v1/projects/:project_id",
        Router::new()
            .route(
                "/queries/event-segmentation",
                routing::post(event_segmentation),
            )
    )
}
