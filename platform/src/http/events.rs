use crate::{
    Context, Result,
};
use axum::{extract::Extension, routing::post, routing::get, Json, Router, routing};
use std::sync::Arc;
use axum::extract::Path;
use metadata::events::Event;
use crate::events::{CreateRequest, Provider, UpdateRequest};

async fn create(
    ctx: Context,
    Extension(provider): Extension<Arc<Provider>>,
    Path(project_id): Path<u64>,
    Json(request): Json<CreateRequest>,
) -> Result<Json<Event>> {
    Ok(Json(provider.create(ctx, project_id, request).await?))
}

async fn get_by_id(
    ctx: Context,
    Extension(provider): Extension<Arc<Provider>>,
    Path(project_id): Path<u64>,
    Path(event_id): Path<u64>,
) -> Result<Json<Event>> {
    Ok(Json(provider.get_by_id(ctx, project_id, event_id).await?))
}

async fn get_by_name(
    ctx: Context,
    Extension(provider): Extension<Arc<Provider>>,
    Path(project_id): Path<u64>,
    Path(event_name): Path<&str>,
) -> Result<Json<Event>> {
    Ok(Json(provider.get_by_name(ctx, project_id, event_name).await?))
}

async fn list(
    ctx: Context,
    Extension(provider): Extension<Arc<Provider>>,
    Path(project_id): Path<u64>,
) -> Result<Json<Vec<Event>>> {
    Ok(Json(provider.list(ctx, project_id).await?))
}

async fn update(
    ctx: Context,
    Extension(provider): Extension<Arc<Provider>>,
    Path(project_id): Path<u64>,
    Path(event_id): Path<u64>,
    Json(request): Json<UpdateRequest>,
) -> Result<Json<Event>> {
    Ok(Json(provider.update(ctx, project_id, event_id, request).await?))
}

async fn delete(
    ctx: Context,
    Extension(provider): Extension<Arc<Provider>>,
    Path(project_id): Path<u64>,
    Path(event_id): Path<u64>,
) -> Result<Json<Event>> {
    Ok(Json(provider.delete(ctx, project_id, event_id).await?))
}

async fn attach_property(
    ctx: Context,
    Extension(provider): Extension<Arc<Provider>>,
    Path(project_id): Path<u64>,
    Path(event_id): Path<u64>,
    Path(prop_id): Path<u64>,
) -> Result<Json<Event>> {
    Ok(Json(provider.attach_property(ctx, project_id, event_id, prop_id).await?))
}

async fn detach_property(
    ctx: Context,
    Extension(provider): Extension<Arc<Provider>>,
    Path(project_id): Path<u64>,
    Path(event_id): Path<u64>,
    Path(prop_id): Path<u64>,
) -> Result<Json<Event>> {
    Ok(Json(provider.detach_property(ctx, project_id, event_id, prop_id).await?))
}

pub fn configure(router: Router) -> Router {
    router
        .route("/v1/projects/:project_id/events", routing::post(create).routing::get(list))
        .route("/v1/projects/:project_id/events/:event_id", routing::get(get_by_id).routing::update(update).routing::delete(delete))
        .route("/v1/projects/:project_id/events/name/:event_name", routing::get(get_by_name))
        .route("/v1/projects/:project_id/events/:event_id/properties/:property_id", routing::put(attach_property).routing::delete(detach_propert))
}
