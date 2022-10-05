use crate::accounts::{Account, CreateAccountRequest, UpdateAccountRequest};
use crate::{AccountsProvider, Context, Result};
use axum::extract::Path;

use axum::{extract::Extension, routing, Json, Router};

use axum::http::StatusCode;
use metadata::metadata::ListResponse;
use std::sync::Arc;

async fn create(
    ctx: Context,
    Extension(provider): Extension<Arc<AccountsProvider>>,
    Json(req): Json<CreateAccountRequest>,
) -> Result<(StatusCode, Json<Account>)> {
    Ok((StatusCode::CREATED, Json(provider.create(ctx, req).await?)))
}

async fn get_by_id(
    ctx: Context,
    Extension(provider): Extension<Arc<AccountsProvider>>,
    Path(id): Path<u64>,
) -> Result<Json<Account>> {
    Ok(Json(provider.get_by_id(ctx, id).await?))
}

async fn list(
    ctx: Context,
    Extension(provider): Extension<Arc<AccountsProvider>>,
) -> Result<Json<ListResponse<Account>>> {
    Ok(Json(provider.list(ctx).await?))
}

async fn update(
    ctx: Context,
    Extension(provider): Extension<Arc<AccountsProvider>>,
    Path(id): Path<u64>,
    Json(request): Json<UpdateAccountRequest>,
) -> Result<Json<Account>> {
    Ok(Json(provider.update(ctx, id, request).await?))
}

async fn delete(
    ctx: Context,
    Extension(provider): Extension<Arc<AccountsProvider>>,
    Path(id): Path<u64>,
) -> Result<Json<Account>> {
    Ok(Json(provider.delete(ctx, id).await?))
}

pub fn attach_routes(
    router: Router,
    accounts: Arc<AccountsProvider>,
    md_accounts: Arc<metadata::accounts::Provider>,
) -> Router {
    router
        .route("/v1/accounts", routing::post(create).get(list))
        .route(
            "/v1/accounts/:id",
            routing::get(get_by_id).delete(delete).put(update),
        )
        .layer(Extension(md_accounts))
        .layer(Extension(accounts))
}
