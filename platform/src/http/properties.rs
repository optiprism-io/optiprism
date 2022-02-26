use crate::properties::Provider;
use crate::{Context, Result};
use axum::extract::Path;
use axum::{extract::Extension, routing, Json, Router};
use metadata::metadata::ListResponse;
use metadata::properties::provider::Namespace;
use metadata::properties::Property;
use std::sync::Arc;

async fn get_by_id(
    ctx: Context,
    Extension(provider): Extension<Arc<Provider>>,
    Path((project_id, event_id)): Path<(u64, u64)>,
) -> Result<Json<Property>> {
    Ok(Json(provider.get_by_id(ctx, project_id, event_id).await?))
}

async fn get_by_name(
    ctx: Context,
    Extension(provider): Extension<Arc<Provider>>,
    Path((project_id, prop_name)): Path<(u64, String)>,
) -> Result<Json<Property>> {
    Ok(Json(
        provider.get_by_name(ctx, project_id, &prop_name).await?,
    ))
}

async fn list(
    ctx: Context,
    Extension(provider): Extension<Arc<Provider>>,
    Path(project_id): Path<u64>,
) -> Result<Json<ListResponse<Property>>> {
    Ok(Json(provider.list(ctx, project_id).await?))
}

async fn delete(
    ctx: Context,
    Extension(provider): Extension<Arc<Provider>>,
    Path((project_id, event_id)): Path<(u64, u64)>,
) -> Result<Json<Property>> {
    Ok(Json(provider.delete(ctx, project_id, event_id).await?))
}

pub fn configure(router: Router, ns: Namespace) -> Router {
    match ns {
        Namespace::Event => router
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
            ),
        Namespace::User => router
            .route(
                "/v1/projects/:project_id/user_properties",
                routing::get(list),
            )
            .route(
                "/v1/projects/:project_id/user_properties/:prop_id",
                routing::get(get_by_id).delete(delete),
            )
            .route(
                "/v1/projects/:project_id/user_properties/name/:prop_name",
                routing::get(get_by_name),
            ),
    }
}
