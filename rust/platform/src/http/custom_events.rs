use crate::{custom_events, Context, Result};
use axum::extract::Path;

use crate::custom_events::types::{
    CreateCustomEventRequest, CustomEvent, UpdateCustomEventRequest,
};
use axum::http::StatusCode;
use axum::{extract::Extension, routing, Json, Router};
use metadata::metadata::ListResponse;
use std::sync::Arc;

async fn create(
    ctx: Context,
    Extension(provider): Extension<Arc<custom_events::Provider>>,
    Path((organization_id, project_id)): Path<(u64, u64)>,
    Json(request): Json<CreateCustomEventRequest>,
) -> Result<(StatusCode, Json<CustomEvent>)> {
    Ok((
        StatusCode::CREATED,
        Json(
            provider
                .create(ctx, organization_id, project_id, request)
                .await?,
        ),
    ))
}

async fn get_by_id(
    ctx: Context,
    Extension(provider): Extension<Arc<custom_events::Provider>>,
    Path((organization_id, project_id, event_id)): Path<(u64, u64, u64)>,
) -> Result<Json<CustomEvent>> {
    Ok(Json(
        provider
            .get_by_id(ctx, organization_id, project_id, event_id)
            .await?,
    ))
}

async fn list(
    ctx: Context,
    Extension(provider): Extension<Arc<custom_events::Provider>>,
    Path((organization_id, project_id)): Path<(u64, u64)>,
) -> Result<Json<ListResponse<CustomEvent>>> {
    Ok(Json(provider.list(ctx, organization_id, project_id).await?))
}

async fn update(
    ctx: Context,
    Extension(provider): Extension<Arc<custom_events::Provider>>,
    Path((organization_id, project_id, event_id)): Path<(u64, u64, u64)>,
    Json(request): Json<UpdateCustomEventRequest>,
) -> Result<Json<CustomEvent>> {
    Ok(Json(
        provider
            .update(ctx, organization_id, project_id, event_id, request)
            .await?,
    ))
}

async fn delete(
    ctx: Context,
    Extension(provider): Extension<Arc<custom_events::Provider>>,
    Path((organization_id, project_id, event_id)): Path<(u64, u64, u64)>,
) -> Result<Json<CustomEvent>> {
    Ok(Json(
        provider
            .delete(ctx, organization_id, project_id, event_id)
            .await?,
    ))
}

pub fn attach_routes(router: Router, events: Arc<custom_events::Provider>) -> Router {
    router
        .route(
            "/v1/organizations/:organization_id/projects/:project_id/schema/custom-events",
            routing::post(create).get(list),
        )
        .route(
            "/v1/organizations/:organization_id/projects/:project_id/schema/custom-events/:event_id",
            routing::get(get_by_id).delete(delete).put(update),
        )
        .layer(Extension(events))
}
