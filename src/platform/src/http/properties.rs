use std::sync::Arc;

use axum::extract::Extension;
use axum::extract::Path;
use axum::routing;
use axum::Router;

use crate::http::Json;
use crate::properties;
use crate::properties::Property;
use crate::properties::UpdatePropertyRequest;
use crate::Context;
use crate::ListResponse;
use crate::Result;

async fn get_by_id(
    ctx: Context,
    Extension(provider): Extension<Arc<dyn properties::Provider>>,
    Path((organization_id, project_id, property_id)): Path<(u64, u64, u64)>,
) -> Result<Json<Property>> {
    Ok(Json(
        provider
            .get_by_id(ctx, organization_id, project_id, property_id)
            .await?,
    ))
}

async fn get_by_name(
    ctx: Context,
    Extension(provider): Extension<Arc<dyn properties::Provider>>,
    Path((organization_id, project_id, prop_name)): Path<(u64, u64, String)>,
) -> Result<Json<Property>> {
    Ok(Json(
        provider
            .get_by_name(ctx, organization_id, project_id, &prop_name)
            .await?,
    ))
}

async fn list(
    ctx: Context,
    Extension(provider): Extension<Arc<dyn properties::Provider>>,
    Path((organization_id, project_id)): Path<(u64, u64)>,
) -> Result<Json<ListResponse<Property>>> {
    Ok(Json(provider.list(ctx, organization_id, project_id).await?))
}

async fn update(
    ctx: Context,
    Extension(provider): Extension<Arc<dyn properties::Provider>>,
    Path((organization_id, project_id, prop_id)): Path<(u64, u64, u64)>,
    Json(request): Json<UpdatePropertyRequest>,
) -> Result<Json<Property>> {
    Ok(Json(
        provider
            .update(ctx, organization_id, project_id, prop_id, request)
            .await?,
    ))
}

async fn delete(
    ctx: Context,
    Extension(provider): Extension<Arc<dyn properties::Provider>>,
    Path((organization_id, project_id, prop_id)): Path<(u64, u64, u64)>,
) -> Result<Json<Property>> {
    Ok(Json(
        provider
            .delete(ctx, organization_id, project_id, prop_id)
            .await?,
    ))
}

pub fn attach_user_routes(router: Router) -> Router {
    router.clone().nest(
        "/organizations/:organization_id/projects/:project_id/schema/user-properties",
        router
            .route("/", routing::get(list))
            .route(
                "/:prop_id",
                routing::get(get_by_id).delete(delete).put(update),
            )
            .route("/name/:prop_name", routing::get(get_by_name)),
    )
}

pub fn attach_event_routes(router: Router) -> Router {
    router.clone().nest(
        "/organizations/:organization_id/projects/:project_id/schema/event-properties",
        Router::new()
            .route("/", routing::get(list))
            .route(
                "/:prop_id",
                routing::get(get_by_id).delete(delete).put(update),
            )
            .route("/name/:prop_name", routing::get(get_by_name)),
    )
}
