use axum::extract::Extension;
use axum::extract::Path;
use axum::routing;
use axum::Router;
use common::http::Json;

use crate::http::PropertiesLayer;
use crate::properties::Property;
use crate::properties::UpdatePropertyRequest;
use crate::Context;
use crate::ListResponse;
use crate::Result;

async fn get_event_by_id(
    ctx: Context,
    Extension(provider): Extension<PropertiesLayer>,
    Path((project_id, property_id)): Path<(u64, u64)>,
) -> Result<Json<Property>> {
    Ok(Json(
        provider
            .event
            .get_by_id(ctx, project_id, property_id)
            .await?,
    ))
}

async fn get_user_by_id(
    ctx: Context,
    Extension(provider): Extension<PropertiesLayer>,
    Path((project_id, property_id)): Path<(u64, u64)>,
) -> Result<Json<Property>> {
    Ok(Json(
        provider
            .user
            .get_by_id(ctx, project_id, property_id)
            .await?,
    ))
}

async fn get_system_by_id(
    ctx: Context,
    Extension(provider): Extension<PropertiesLayer>,
    Path((project_id, property_id)): Path<(u64, u64)>,
) -> Result<Json<Property>> {
    Ok(Json(
        provider
            .system
            .get_by_id(ctx, project_id, property_id)
            .await?,
    ))
}

async fn get_event_by_name(
    ctx: Context,
    Extension(provider): Extension<PropertiesLayer>,
    Path((project_id, prop_name)): Path<(u64, String)>,
) -> Result<Json<Property>> {
    Ok(Json(
        provider
            .event
            .get_by_name(ctx, project_id, &prop_name)
            .await?,
    ))
}

async fn get_user_by_name(
    ctx: Context,
    Extension(provider): Extension<PropertiesLayer>,
    Path((project_id, prop_name)): Path<(u64, String)>,
) -> Result<Json<Property>> {
    Ok(Json(
        provider
            .user
            .get_by_name(ctx, project_id, &prop_name)
            .await?,
    ))
}

async fn get_system_by_name(
    ctx: Context,
    Extension(provider): Extension<PropertiesLayer>,
    Path((project_id, prop_name)): Path<(u64, String)>,
) -> Result<Json<Property>> {
    Ok(Json(
        provider
            .system
            .get_by_name(ctx, project_id, &prop_name)
            .await?,
    ))
}

async fn list_event(
    ctx: Context,
    Extension(provider): Extension<PropertiesLayer>,
    Path(project_id): Path<u64>,
) -> Result<Json<ListResponse<Property>>> {
    Ok(Json(provider.event.list(ctx, project_id).await?))
}

async fn list_user(
    ctx: Context,
    Extension(provider): Extension<PropertiesLayer>,
    Path(project_id): Path<u64>,
) -> Result<Json<ListResponse<Property>>> {
    Ok(Json(provider.user.list(ctx, project_id).await?))
}

async fn list_system(
    ctx: Context,
    Extension(provider): Extension<PropertiesLayer>,
    Path(project_id): Path<u64>,
) -> Result<Json<ListResponse<Property>>> {
    Ok(Json(provider.system.list(ctx, project_id).await?))
}

async fn update_event(
    ctx: Context,
    Extension(provider): Extension<PropertiesLayer>,
    Path((project_id, prop_id)): Path<(u64, u64)>,
    Json(request): Json<UpdatePropertyRequest>,
) -> Result<Json<Property>> {
    Ok(Json(
        provider
            .event
            .update(ctx, project_id, prop_id, request)
            .await?,
    ))
}

async fn update_user(
    ctx: Context,
    Extension(provider): Extension<PropertiesLayer>,
    Path((project_id, prop_id)): Path<(u64, u64)>,
    Json(request): Json<UpdatePropertyRequest>,
) -> Result<Json<Property>> {
    Ok(Json(
        provider
            .user
            .update(ctx, project_id, prop_id, request)
            .await?,
    ))
}

async fn update_system(
    ctx: Context,
    Extension(provider): Extension<PropertiesLayer>,
    Path((project_id, prop_id)): Path<(u64, u64)>,
    Json(request): Json<UpdatePropertyRequest>,
) -> Result<Json<Property>> {
    Ok(Json(
        provider
            .system
            .update(ctx, project_id, prop_id, request)
            .await?,
    ))
}

async fn delete_event(
    ctx: Context,
    Extension(provider): Extension<PropertiesLayer>,
    Path((project_id, prop_id)): Path<(u64, u64)>,
) -> Result<Json<Property>> {
    Ok(Json(provider.event.delete(ctx, project_id, prop_id).await?))
}

async fn delete_user(
    ctx: Context,
    Extension(provider): Extension<PropertiesLayer>,
    Path((project_id, prop_id)): Path<(u64, u64)>,
) -> Result<Json<Property>> {
    Ok(Json(provider.user.delete(ctx, project_id, prop_id).await?))
}

async fn delete_system(
    ctx: Context,
    Extension(provider): Extension<PropertiesLayer>,
    Path((project_id, prop_id)): Path<(u64, u64)>,
) -> Result<Json<Property>> {
    Ok(Json(
        provider.system.delete(ctx, project_id, prop_id).await?,
    ))
}

pub fn attach_user_routes(router: Router) -> Router {
    router.clone().nest(
        "/projects/:project_id/schema/user-properties",
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
        "/projects/:project_id/schema/event-properties",
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

pub fn attach_system_routes(router: Router) -> Router {
    router.clone().nest(
        "/projects/:project_id/schema/system-properties",
        router
            .route("/", routing::get(list_system))
            .route(
                "/:prop_id",
                routing::get(get_system_by_id)
                    .delete(delete_system)
                    .put(update_system),
            )
            .route("/name/:prop_name", routing::get(get_system_by_name)),
    )
}
