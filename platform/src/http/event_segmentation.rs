use std::sync::Arc;
use axum::extract::{Extension, Path};
use axum::{AddExtensionLayer, Json, Router, routing};

use crate::event_segmentation::types::EventSegmentation;
use crate::{Context, EventSegmentationProvider};
use crate::event_segmentation::result::Series;
use crate::Result;

async fn event_segmentation(
    ctx: Context,
    Extension(provider): Extension<Arc<EventSegmentationProvider>>,
    Path(project_id): Path<u64>,
    Json(request): Json<EventSegmentation>,
) -> Result<Json<Series>> {
    Ok(Json(provider.event_segmentation(ctx, project_id, request).await?))
}

pub fn attach_routes(router: Router, prov: Arc<EventSegmentationProvider>) -> Router {
    router
        .route(
            "/v1/projects/:project_id/queries/event-segmentation",
            routing::post(event_segmentation),
        )
        .layer(AddExtensionLayer::new(prov))
}
