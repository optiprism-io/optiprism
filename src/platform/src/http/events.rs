use std::sync::Arc;

use axum::extract::Extension;
use axum::extract::Path;
use axum::http::StatusCode;
use axum::routing;
use axum::Router;
use common::http::Json;

use crate::events::CreateEventRequest;
use crate::events::Event;
use crate::events::Events;
use crate::events::UpdateEventRequest;
use crate::Context;
use crate::ListResponse;
use crate::Result;

async fn create(
    ctx: Context,
    Extension(provider): Extension<Arc<Events>>,
    Path(project_id): Path<u64>,
    Json(request): Json<CreateEventRequest>,
) -> Result<(StatusCode, Json<Event>)> {
    Ok((
        StatusCode::CREATED,
        Json(provider.create(ctx, project_id, request).await?),
    ))
}

async fn get_by_id(
    ctx: Context,
    Extension(provider): Extension<Arc<Events>>,
    Path((project_id, event_id)): Path<(u64, u64)>,
) -> Result<Json<Event>> {
    Ok(Json(provider.get_by_id(ctx, project_id, event_id).await?))
}

async fn get_by_name(
    ctx: Context,
    Extension(provider): Extension<Arc<Events>>,
    Path(project_id): Path<u64>,
    Path(event_name): Path<String>,
) -> Result<Json<Event>> {
    Ok(Json(
        provider.get_by_name(ctx, project_id, &event_name).await?,
    ))
}

async fn list(
    ctx: Context,
    Extension(provider): Extension<Arc<Events>>,
    Path(project_id): Path<u64>,
) -> Result<Json<ListResponse<Event>>> {
    Ok(Json(provider.list(ctx, project_id).await?))
}

async fn update(
    ctx: Context,
    Extension(provider): Extension<Arc<Events>>,
    Path((project_id, event_id)): Path<(u64, u64)>,
    Json(request): Json<UpdateEventRequest>,
) -> Result<Json<Event>> {
    Ok(Json(
        provider.update(ctx, project_id, event_id, request).await?,
    ))
}

async fn delete(
    ctx: Context,
    Extension(provider): Extension<Arc<Events>>,
    Path((project_id, event_id)): Path<(u64, u64)>,
) -> Result<Json<Event>> {
    Ok(Json(provider.delete(ctx, project_id, event_id).await?))
}

async fn attach_property(
    ctx: Context,
    Extension(provider): Extension<Arc<Events>>,
    Path((project_id, event_id, prop_id)): Path<(u64, u64, u64)>,
) -> Result<Json<Event>> {
    Ok(Json(
        provider
            .attach_property(ctx, project_id, event_id, prop_id)
            .await?,
    ))
}

async fn detach_property(
    ctx: Context,
    Extension(provider): Extension<Arc<Events>>,
    Path((project_id, event_id, prop_id)): Path<(u64, u64, u64)>,
) -> Result<Json<Event>> {
    Ok(Json(
        provider
            .detach_property(ctx, project_id, event_id, prop_id)
            .await?,
    ))
}

pub fn attach_routes(router: Router) -> Router {
    router.nest(
        "/projects/:project_id/schema/events",
        Router::new()
            .route("/", routing::post(create).get(list))
            .route(
                "/:event_id",
                routing::get(get_by_id).delete(delete).put(update),
            )
            .route("/name/:event_name", routing::get(get_by_name))
            .route(
                "/:event_id/properties/:property_id",
                routing::post(attach_property),
            )
            .route(
                "/:event_id/properties/:property_id",
                routing::delete(detach_property),
            ),
    )
}
