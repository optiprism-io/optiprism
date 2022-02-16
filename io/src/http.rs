use crate::{events::Request, processing::Provider};
use axum::{
    extract::{Extension, Path},
    routing::post,
    Json, Router,
};
use std::sync::Arc;

#[axum_debug::debug_handler]
async fn ingest(
    Extension(provider): Extension<Arc<Provider>>,
    Path(id): Path<String>,
    Json(request): Json<Request>,
) -> Result<Json<Response>> {
    Ok(Json(provider.ingest(id, request).await?))
}

pub fn configure(router: Router) -> Router {
    router.route("/v1/projects/:id/ingest/events", post(ingest))
}
