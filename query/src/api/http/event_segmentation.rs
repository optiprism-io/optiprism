use std::sync::Arc;
use axum::extract::{Extension, Path};
use axum::Json;
use metadata::Metadata;
use crate::Context;
use crate::event_segmentation::EventSegmentation;

async fn handler(
    ctx: Context,
    Extension(metadata): Extension<Arc<Metadata>>,
    Path(project_id): Path<u64>,
    Json(request): Json<EventSegmentation>,
) -> Result<Json<Event>> {
    if request.project_id != project_id {
        return Err(Error::Internal(InternalError::new(
            "wrong project id",
            StatusCode::BAD_REQUEST,
        )));
    }

    Ok(Json(provider.create(ctx, request).await?))
}