use std::sync::Arc;

use axum::extract::Extension;
use axum::extract::Path;
use axum::routing;
use axum::Router;
use common::http::Json;

use crate::http::Properties;
use crate::properties;
use crate::properties::Property;
use crate::properties::UpdatePropertyRequest;
use crate::Context;
use crate::ListResponse;
use crate::Result;

async fn get_event_by_id(
    ctx: Context,
    Extension(provider): Extension<Properties>,
    Path((organization_id, project_id, property_id)): Path<(u64, u64, u64)>,
) -> Result<Json<Property>> {
    Ok(Json(
        provider
            .event
            .get_by_id(ctx, organization_id, project_id, property_id)
            .await?,
    ))
}

async fn get_user_by_id(
    ctx: Context,
    Extension(provider): Extension<Properties>,
    Path((organization_id, project_id, property_id)): Path<(u64, u64, u64)>,
) -> Result<Json<Property>> {
    Ok(Json(
        provider
            .user
            .get_by_id(ctx, organization_id, project_id, property_id)
            .await?,
    ))
}

async fn get_event_by_name(
    ctx: Context,
    Extension(provider): Extension<Properties>,
    Path((organization_id, project_id, prop_name)): Path<(u64, u64, String)>,
) -> Result<Json<Property>> {
    Ok(Json(
        provider
            .event
            .get_by_name(ctx, organization_id, project_id, &prop_name)
            .await?,
    ))
}

async fn get_user_by_name(
    ctx: Context,
    Extension(provider): Extension<Properties>,
    Path((organization_id, project_id, prop_name)): Path<(u64, u64, String)>,
) -> Result<Json<Property>> {
    Ok(Json(
        provider
            .user
            .get_by_name(ctx, organization_id, project_id, &prop_name)
            .await?,
    ))
}

async fn list_event(
    ctx: Context,
    Extension(provider): Extension<Properties>,
    Path((organization_id, project_id)): Path<(u64, u64)>,
) -> Result<Json<ListResponse<Property>>> {
    Ok(Json(
        provider
            .event
            .list(ctx, organization_id, project_id)
            .await?,
    ))
}

async fn list_user(
    ctx: Context,
    Extension(provider): Extension<Properties>,
    Path((organization_id, project_id)): Path<(u64, u64)>,
) -> Result<Json<ListResponse<Property>>> {
    Ok(Json(
        provider.user.list(ctx, organization_id, project_id).await?,
    ))
}

async fn update_event(
    ctx: Context,
    Extension(provider): Extension<Properties>,
    Path((organization_id, project_id, prop_id)): Path<(u64, u64, u64)>,
    Json(request): Json<UpdatePropertyRequest>,
) -> Result<Json<Property>> {
    Ok(Json(
        provider
            .event
            .update(ctx, organization_id, project_id, prop_id, request)
            .await?,
    ))
}

async fn update_user(
    ctx: Context,
    Extension(provider): Extension<Properties>,
    Path((organization_id, project_id, prop_id)): Path<(u64, u64, u64)>,
    Json(request): Json<UpdatePropertyRequest>,
) -> Result<Json<Property>> {
    Ok(Json(
        provider
            .user
            .update(ctx, organization_id, project_id, prop_id, request)
            .await?,
    ))
}

async fn delete_event(
    ctx: Context,
    Extension(provider): Extension<Properties>,
    Path((organization_id, project_id, prop_id)): Path<(u64, u64, u64)>,
) -> Result<Json<Property>> {
    Ok(Json(
        provider
            .event
            .delete(ctx, organization_id, project_id, prop_id)
            .await?,
    ))
}

async fn delete_user(
    ctx: Context,
    Extension(provider): Extension<Properties>,
    Path((organization_id, project_id, prop_id)): Path<(u64, u64, u64)>,
) -> Result<Json<Property>> {
    Ok(Json(
        provider
            .user
            .delete(ctx, organization_id, project_id, prop_id)
            .await?,
    ))
}

pub fn attach_user_routes(router: Router) -> Router {
    router.clone().nest(
        "/organizations/:organization_id/projects/:project_id/schema/user-properties",
        router
            .route("/", routing::get(list_user))
            .route(
                "/:prop_id",
                routing::get(get_user_by_id)
                    .delete(delete_user)
                    .put(update_user),
            )
            .route("/name/:prop_name", routing::get(get_user_by_name)),
    )
}

pub fn attach_event_routes(router: Router) -> Router {
    router.nest(
        "/organizations/:organization_id/projects/:project_id/schema/event-properties",
        Router::new()
            .route("/", routing::get(list_event))
            .route(
                "/:prop_id",
                routing::get(get_event_by_id)
                    .delete(delete_event)
                    .put(update_event),
            )
            .route("/name/:prop_name", routing::get(get_event_by_name)),
    )
}
