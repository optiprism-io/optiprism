use crate::{
    auth::types::{LogInRequest, SignUpRequest},
    AuthProvider, Context, Result,
};
use axum::{extract::Extension, routing::post, AddExtensionLayer, Json, Router};
use std::sync::Arc;
use crate::auth::types::TokenResponse;

async fn sign_up(
    ctx: Context,
    Extension(provider): Extension<Arc<AuthProvider>>,
    Json(request): Json<SignUpRequest>,
) -> Result<Json<TokenResponse>> {
    Ok(Json(provider.sign_up(ctx, request).await?))
}

#[axum_debug::debug_handler]
async fn log_in(
    ctx: Context,
    Extension(provider): Extension<Arc<AuthProvider>>,
    Json(request): Json<LogInRequest>,
) -> Result<Json<TokensResponse>> {
    Ok(Json(provider.log_in(ctx, request).await?))
}

pub fn attach_routes(router: Router, auth: Arc<AuthProvider>) -> Router {
    router
        .route("/v1/auth/signup", post(sign_up))
        .route("/v1/auth/login", post(log_in))
        .layer(AddExtensionLayer::new(auth))
}
