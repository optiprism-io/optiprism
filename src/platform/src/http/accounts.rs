use std::sync::Arc;

use axum::extract::Extension;
use axum::extract::Path;
use axum::http::StatusCode;
use axum::routing;
use axum::Router;

use crate::accounts;
use crate::accounts::Account;
use crate::accounts::CreateAccountRequest;
use crate::accounts::UpdateAccountRequest;
use crate::http::Json;
use crate::Context;
use crate::ListResponse;
use crate::Result;

async fn create(
    ctx: Context,
    Extension(provider): Extension<Arc<dyn accounts::Provider>>,
    Json(req): Json<CreateAccountRequest>,
) -> Result<(StatusCode, Json<Account>)> {
    Ok((StatusCode::CREATED, Json(provider.create(ctx, req).await?)))
}

async fn get_by_id(
    ctx: Context,
    Extension(provider): Extension<Arc<dyn accounts::Provider>>,
    Path(id): Path<u64>,
) -> Result<Json<Account>> {
    Ok(Json(provider.get_by_id(ctx, id).await?))
}

async fn list(
    ctx: Context,
    Extension(provider): Extension<Arc<dyn accounts::Provider>>,
) -> Result<Json<ListResponse<Account>>> {
    Ok(Json(provider.list(ctx).await?))
}

async fn update(
    ctx: Context,
    Extension(provider): Extension<Arc<dyn accounts::Provider>>,
    Path(id): Path<u64>,
    Json(request): Json<UpdateAccountRequest>,
) -> Result<Json<Account>> {
    Ok(Json(provider.update(ctx, id, request).await?))
}

async fn delete(
    ctx: Context,
    Extension(provider): Extension<Arc<dyn accounts::Provider>>,
    Path(id): Path<u64>,
) -> Result<Json<Account>> {
    Ok(Json(provider.delete(ctx, id).await?))
}

pub fn attach_routes(router: Router) -> Router {
    router.nest(
        "/accounts",
        Router::new()
            .route("/", routing::post(create).get(list))
            .route("/:id", routing::get(get_by_id).delete(delete).put(update)),
    )
}
