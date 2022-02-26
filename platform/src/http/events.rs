use crate::error::{Error, InternalError};
use crate::events::{CreateRequest, Provider, UpdateRequest};
use crate::{Context, Result};
use axum::extract::Path;
use axum::http::StatusCode;
use axum::{extract::Extension, routing, Json, Router};
use metadata::events::Event;
use metadata::metadata::ListResponse;
use std::sync::Arc;

async fn create(
    ctx: Context,
    Extension(provider): Extension<Arc<Provider>>,
    Path(project_id): Path<u64>,
    Json(request): Json<CreateRequest>,
) -> Result<Json<Event>> {
    if request.project_id != project_id {
        return Err(Error::Internal(InternalError::new(
            "wrong project id",
            StatusCode::BAD_REQUEST,
        )));
    }

    Ok(Json(provider.create(ctx, request).await?))
}

async fn get_by_id(
    ctx: Context,
    Extension(provider): Extension<Arc<Provider>>,
    Path((project_id, event_id)): Path<(u64, u64)>,
) -> Result<Json<Event>> {
    Ok(Json(provider.get_by_id(ctx, project_id, event_id).await?))
}

async fn get_by_name(
    ctx: Context,
    Extension(provider): Extension<Arc<Provider>>,
    Path((project_id, event_name)): Path<(u64, String)>,
) -> Result<Json<Event>> {
    Ok(Json(
        provider.get_by_name(ctx, project_id, &event_name).await?,
    ))
}

async fn list(
    ctx: Context,
    Extension(provider): Extension<Arc<Provider>>,
    Path(project_id): Path<u64>,
) -> Result<Json<ListResponse<Event>>> {
    Ok(Json(provider.list(ctx, project_id).await?))
}

async fn update(
    ctx: Context,
    Extension(provider): Extension<Arc<Provider>>,
    Path((project_id, event_id)): Path<(u64, u64)>,
    Json(request): Json<UpdateRequest>,
) -> Result<Json<Event>> {
    if request.project_id != project_id {
        return Err(Error::Internal(InternalError::new(
            "code",
            StatusCode::BAD_REQUEST,
        )));
    }
    if request.id != event_id {
        return Err(Error::Internal(InternalError::new(
            "code",
            StatusCode::BAD_REQUEST,
        )));
    }

    Ok(Json(provider.update(ctx, request).await?))
}

async fn delete(
    ctx: Context,
    Extension(provider): Extension<Arc<Provider>>,
    Path((project_id, event_id)): Path<(u64, u64)>,
) -> Result<Json<Event>> {
    Ok(Json(provider.delete(ctx, project_id, event_id).await?))
}

async fn attach_property(
    ctx: Context,
    Extension(provider): Extension<Arc<Provider>>,
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
    Extension(provider): Extension<Arc<Provider>>,
    Path((project_id, event_id, prop_id)): Path<(u64, u64, u64)>,
) -> Result<Json<Event>> {
    Ok(Json(
        provider
            .detach_property(ctx, project_id, event_id, prop_id)
            .await?,
    ))
}

pub fn configure(router: Router) -> Router {
    router
        .route(
            "/v1/projects/:project_id/events",
            routing::post(create).get(list),
        )
        .route(
            "/v1/projects/:project_id/events/:event_id",
            routing::get(get_by_id).delete(delete).put(update),
        )
        .route(
            "/v1/projects/:project_id/events/name/:event_name",
            routing::get(get_by_name),
        )
        .route(
            "/v1/projects/:project_id/events/:event_id/properties/:property_id",
            routing::post(attach_property),
        )
        .route(
            "/v1/projects/:project_id/events/:event_id/properties/:property_id",
            routing::delete(detach_property),
        )
}
