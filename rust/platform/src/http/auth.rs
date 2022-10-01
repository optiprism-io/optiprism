use crate::{
    auth::types::{LogInRequest, SignUpRequest},
    AuthProvider, Context, Result,
};
use axum::{extract::Extension, routing::post, AddExtensionLayer, Json, Router};
use std::sync::Arc;
use crate::auth::types::TokenResponse;

async fn sign_up(
    Extension(provider): Extension<Arc<AuthProvider>>,
    Json(request): Json<SignUpRequest>,
) -> Result<Json<TokenResponse>> {
    Ok(Json(provider.sign_up(request).await?))
}

#[axum_debug::debug_handler]
async fn log_in(
    Extension(provider): Extension<Arc<AuthProvider>>,
    Json(request): Json<LogInRequest>,
) -> Result<Json<TokenResponse>> {
    Ok(Json(provider.log_in(request).await?))
}

// String works too
async fn dd() -> String {
    "Hello, World!".to_string()
}

pub fn attach_routes(router: Router, auth: Arc<AuthProvider>) -> Router {
    router
        .route("/v1/auth/signup", post(sign_up))
        .route("/v1/auth/login", post(log_in))
        .layer(AddExtensionLayer::new(auth))
}
