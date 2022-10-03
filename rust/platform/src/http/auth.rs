use crate::auth::types::TokensResponse;
use crate::{auth::types::SignUpRequest, AuthProvider, Result};

use axum::{extract::Extension, routing::post, AddExtensionLayer, Json, Router};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LogInRequest {
    pub email: String,
    pub password: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RefreshTokensRequest {
    pub refresh_token: String,
}

async fn sign_up(
    Extension(provider): Extension<Arc<AuthProvider>>,
    Json(req): Json<SignUpRequest>,
) -> Result<(StatusCode, Json<TokensResponse>)> {
    Ok((StatusCode::CREATED, Json(provider.sign_up(req).await?)))
}

async fn log_in(
    Extension(provider): Extension<Arc<AuthProvider>>,
    Json(req): Json<LogInRequest>,
) -> Result<Json<TokensResponse>> {
    Ok(Json(
        provider
            .log_in(req.email.as_str(), req.password.as_str())
            .await?,
    ))
}

async fn refresh_token(
    Extension(provider): Extension<Arc<AuthProvider>>,
    Json(req): Json<RefreshTokensRequest>,
) -> Result<Json<TokensResponse>> {
    Ok(Json(
        provider.refresh_token(req.refresh_token.as_str()).await?,
    ))
}

pub fn attach_routes(router: Router, auth: Arc<AuthProvider>) -> Router {
    router
        .route("/v1/auth/signup", post(sign_up))
        .route("/v1/auth/login", post(log_in))
        .route("/v1/auth/refresh-token", post(refresh_token))
        .layer(AddExtensionLayer::new(auth))
}
