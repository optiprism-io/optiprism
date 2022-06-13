use crate::error::{Error, InternalError};
use crate::events::{CreateRequest, Provider, UpdateRequest};
use crate::{Context, events, EventsProvider, Result};
use axum::extract::Path;
use axum::http::StatusCode;
use axum::{extract::Extension, routing, Json, Router, AddExtensionLayer};
use metadata::events::Event;
use metadata::metadata::ListResponse;
use std::sync::Arc;

async fn create(
    ctx: Context,
    Extension(provider): Extension<Arc<EventsProvider>>,
    Path((organization_id, project_id)): Path<(u64, u64)>,
    Json(request): Json<CreateRequest>,
) -> Result<Json<Event>> {
    Ok(Json(provider.create(ctx, organization_id, project_id, request).await?))
}

async fn get_by_id(
    ctx: Context,
    Extension(provider): Extension<Arc<EventsProvider>>,
    Path((organization_id, project_id, event_id)): Path<(u64, u64, u64)>,
) -> Result<Json<Event>> {
    Ok(Json(provider.get_by_id(ctx, organization_id, project_id, event_id).await?))
}

async fn get_by_name(
    ctx: Context,
    Extension(provider): Extension<Arc<EventsProvider>>,
    Path((organization_id, project_id)): Path<(u64, u64)>,
    Path(event_name): Path<String>,
) -> Result<Json<Event>> {
    Ok(Json(
        provider.get_by_name(ctx, organization_id, project_id, &event_name).await?,
    ))
}

async fn list(
    ctx: Context,
    Extension(provider): Extension<Arc<EventsProvider>>,
    Path((organization_id, project_id)): Path<(u64, u64)>,
) -> Result<Json<ListResponse<Event>>> {
    Ok(Json(provider.list(ctx, organization_id, project_id).await?))
}

async fn update(
    ctx: Context,
    Extension(provider): Extension<Arc<EventsProvider>>,
    Path((organization_id, project_id, event_id)): Path<(u64, u64, u64)>,
    Json(request): Json<UpdateRequest>,
) -> Result<Json<Event>> {
    Ok(Json(provider.update(ctx, organization_id, project_id, event_id, request).await?))
}

async fn delete(
    ctx: Context,
    Extension(provider): Extension<Arc<EventsProvider>>,
    Path((organization_id, project_id, event_id)): Path<(u64, u64, u64)>,
) -> Result<Json<Event>> {
    Ok(Json(provider.delete(ctx, organization_id, project_id, event_id).await?))
}

async fn attach_property(
    ctx: Context,
    Extension(provider): Extension<Arc<EventsProvider>>,
    Path((organization_id, project_id, event_id, prop_id)): Path<(u64, u64, u64, u64)>,
) -> Result<Json<Event>> {
    Ok(Json(
        provider
            .attach_property(ctx, organization_id, project_id, event_id, prop_id)
            .await?,
    ))
}

async fn detach_property(
    ctx: Context,
    Extension(provider): Extension<Arc<EventsProvider>>,
    Path((organization_id, project_id, event_id, prop_id)): Path<(u64, u64, u64, u64)>,
) -> Result<Json<Event>> {
    Ok(Json(
        provider
            .detach_property(ctx, organization_id, project_id, event_id, prop_id)
            .await?,
    ))
}

pub fn attach_routes(router: Router, events: Arc<EventsProvider>) -> Router {
    router
        .route(
            "/v1/organizations/:organization_id/projects/:project_id/events",
            routing::post(create).get(list),
        )
        .route(
            "/v1/organizations/:organization_id/projects/:project_id/events/:event_id",
            routing::get(get_by_id).delete(delete).put(update),
        )
        .route(
            "/v1/organizations/:organization_id/projects/:project_id/events/name/:event_name",
            routing::get(get_by_name),
        )
        .route(
            "/v1/organizations/:organization_id/projects/:project_id/events/:event_id/properties/:property_id",
            routing::post(attach_property),
        )
        .route(
            "/v1/organizations/:organization_id/projects/:project_id/events/:event_id/properties/:property_id",
            routing::delete(detach_property),
        )
        .layer(AddExtensionLayer::new(events))
}
