use crate::{
    auth::{
        provider::Provider,
        types::{LogInRequest, SignUpRequest, TokensResponse},
    },
    context::Context,
    error::Result,
};
use axum::{extract::Extension, routing::post, Json, Router};

async fn sign_up(
    ctx: Context,
    Extension(provider): Extension<Provider>,
    Json(request): Json<SignUpRequest>,
) -> Result<Json<TokensResponse>> {
    Ok(Json(provider.sign_up(ctx, request).await?))
}

async fn log_in(
    ctx: Context,
    Extension(provider): Extension<Provider>,
    Json(request): Json<LogInRequest>,
) -> Result<Json<TokensResponse>> {
    Ok(Json(provider.log_in(ctx, request).await?))
}

pub fn configure(router: &mut Router) {
    router
        .route("/v1/auth/signup", post(sign_up))
        .route("/v1/auth/login", post(log_in));
}
