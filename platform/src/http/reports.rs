use std::sync::Arc;
use axum::extract::{Extension};
use axum::{AddExtensionLayer, Json, Router, routing};

use crate::event_segmentation::types::EventSegmentation;
use query::reports::results::Series;
use crate::{Context, ReportsProvider};
use crate::Result;

async fn event_segmentation(
    ctx: Context,
    Extension(provider): Extension<Arc<ReportsProvider>>,
    Json(request): Json<EventSegmentation>,
) -> Result<Json<Series>> {
    Ok(Json(provider.event_segmentation(ctx, request).await?))
}

pub fn attach_routes(router: Router, prov: Arc<ReportsProvider>) -> Router {
    router
        .route(
            "/v1/projects/:project_id/queries/event-segmentation",
            routing::post(event_segmentation),
        )
        .layer(AddExtensionLayer::new(prov))
}
