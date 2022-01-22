use crate::{
    auth::{
        provider::Provider,
        types::{LogInRequest, SignUpRequest, TokensResponse},
    },
    Context, Result,
};
use axum::{extract::Extension, routing::post, Json, Router};
use std::sync::Arc;

async fn sign_up(
    ctx: Context,
    Extension(provider): Extension<Arc<Provider>>,
    Json(request): Json<SignUpRequest>,
) -> Result<Json<TokensResponse>> {
    Ok(Json(provider.sign_up(ctx, request).await?))
}

async fn log_in(
    ctx: Context,
    Extension(provider): Extension<Arc<Provider>>,
    Json(request): Json<LogInRequest>,
) -> Result<Json<TokensResponse>> {
    Ok(Json(provider.log_in(ctx, request).await?))
}

pub fn configure(router: Router) -> Router {
    router
        .route("/v1/auth/signup", post(sign_up))
        .route("/v1/auth/login", post(log_in))
}
