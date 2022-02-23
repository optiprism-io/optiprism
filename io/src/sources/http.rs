use super::Provider;
use crate::Result;
use axum::{
    extract::{Extension, Path},
    routing::post,
    Json, Router,
};
use metadata::events_queue::EventWithContext;
use std::sync::Arc;

#[axum_debug::debug_handler]
async fn ingest(
    Extension(provider): Extension<Arc<Provider>>,
    Path(id): Path<u64>,
    Json(mut source): Json<EventWithContext>,
) -> Result<()> {
    source.project_id = id;
    Ok(provider.ingest(source).await?)
}

pub fn configure(router: Router) -> Router {
    router.route("/v1/projects/:id/ingest/events", post(ingest))
}
