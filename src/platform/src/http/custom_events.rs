use std::sync::Arc;

use axum::extract::Extension;
use axum::extract::Path;
use axum::http::StatusCode;
use axum::routing;
use axum::Router;
use common::http::Json;

use crate::custom_events::CreateCustomEventRequest;
use crate::custom_events::CustomEvent;
use crate::custom_events::CustomEvents;
use crate::custom_events::UpdateCustomEventRequest;
use crate::Context;
use crate::ListResponse;
use crate::Result;

async fn create(
    ctx: Context,
    Extension(provider): Extension<Arc<CustomEvents>>,
    Path(project_id): Path<u64>,
    Json(request): Json<CreateCustomEventRequest>,
) -> Result<(StatusCode, Json<CustomEvent>)> {
    Ok((
        StatusCode::CREATED,
        Json(provider.create(ctx, project_id, request).await?),
    ))
}

async fn get_by_id(
    ctx: Context,
    Extension(provider): Extension<Arc<CustomEvents>>,
    Path((project_id, event_id)): Path<(u64, u64)>,
) -> Result<Json<CustomEvent>> {
    Ok(Json(provider.get_by_id(ctx, project_id, event_id).await?))
}

async fn list(
    ctx: Context,
    Extension(provider): Extension<Arc<CustomEvents>>,
    Path(project_id): Path<u64>,
) -> Result<Json<ListResponse<CustomEvent>>> {
    Ok(Json(provider.list(ctx, project_id).await?))
}

async fn update(
    ctx: Context,
    Extension(provider): Extension<Arc<CustomEvents>>,
    Path((project_id, event_id)): Path<(u64, u64)>,
    Json(request): Json<UpdateCustomEventRequest>,
) -> Result<Json<CustomEvent>> {
    Ok(Json(
        provider.update(ctx, project_id, event_id, request).await?,
    ))
}

async fn delete(
    ctx: Context,
    Extension(provider): Extension<Arc<CustomEvents>>,
    Path((project_id, event_id)): Path<(u64, u64)>,
) -> Result<Json<CustomEvent>> {
    Ok(Json(provider.delete(ctx, project_id, event_id).await?))
}

pub fn attach_routes(router: Router) -> Router {
    router.clone().nest(
        "/api/v1/projects/:project_id/schema/custom-events",
        router.route("/", routing::post(create).get(list)).route(
            "/:event_id",
            routing::get(get_by_id).delete(delete).put(update),
        ),
    )
}
