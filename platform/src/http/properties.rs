use crate::properties::{Provider, UpdateRequest};
use crate::{Context, properties, PropertiesProvider, Result};
use axum::extract::Path;
use axum::{extract::Extension, routing, Json, Router, AddExtensionLayer};
use metadata::metadata::ListResponse;
use metadata::properties::Property;
use std::sync::Arc;

async fn get_by_id(
    ctx: Context,
    Extension(provider): Extension<Arc<PropertiesProvider>>,
    Path((organization_id, project_id, property_id)): Path<(u64, u64, u64)>,
) -> Result<Json<Property>> {
    Ok(Json(provider.get_by_id(ctx, organization_id, project_id, property_id).await?))
}

async fn get_by_name(
    ctx: Context,
    Extension(provider): Extension<Arc<PropertiesProvider>>,
    Path((organization_id, project_id, prop_name)): Path<(u64, u64, String)>,
) -> Result<Json<Property>> {
    Ok(Json(
        provider.get_by_name(ctx, organization_id, project_id, &prop_name).await?,
    ))
}

async fn list(
    ctx: Context,
    Extension(provider): Extension<Arc<PropertiesProvider>>,
    Path((organization_id, project_id)): Path<(u64, u64)>,
) -> Result<Json<ListResponse<Property>>> {
    Ok(Json(provider.list(ctx, organization_id, project_id).await?))
}

async fn update(
    ctx: Context,
    Extension(provider): Extension<Arc<PropertiesProvider>>,
    Path((organization_id, project_id, event_id)): Path<(u64, u64, u64)>,
    Json(request): Json<UpdateRequest>,
) -> Result<Json<Property>> {
    Ok(Json(provider.update(ctx, organization_id, project_id, event_id, request).await?))
}

async fn delete(
    ctx: Context,
    Extension(provider): Extension<Arc<PropertiesProvider>>,
    Path((organization_id,project_id, event_id)): Path<(u64,u64, u64)>,
) -> Result<Json<Property>> {
    Ok(Json(provider.delete(ctx, organization_id,project_id, event_id).await?))
}

pub fn attach_user_routes(router: Router, prop: Arc<properties::Provider>) -> Router {
    router
        .route(
            "/v1/organizations/:organization_id/projects/:project_id/schema/user_properties",
            routing::get(list),
        )
        .route(
            "/v1/organizations/:organization_id/projects/:project_id/schema/user_properties/:prop_id",
            routing::get(get_by_id).delete(delete).put(update),
        )
        .route(
            "/v1/organizations/:organization_id/projects/:project_id/schema/user_properties/name/:prop_name",
            routing::get(get_by_name),
        )
        .layer(AddExtensionLayer::new(prop))
}

pub fn attach_event_routes(router: Router, prop: Arc<properties::Provider>) -> Router {
    router
        .route(
            "/v1/projects/:project_id/event_properties",
            routing::get(list),
        )
        .route(
            "/v1/projects/:project_id/event_properties/:prop_id",
            routing::get(get_by_id).delete(delete),
        )
        .route(
            "/v1/projects/:project_id/event_properties/name/:prop_name",
            routing::get(get_by_name),
        )
        .layer(AddExtensionLayer::new(prop))
}
