use std::sync::Arc;

use axum::extract::Extension;
use axum::http::Response;
use axum::routing::post;
use axum::Router;
use axum_macros::debug_handler;
use reqwest::StatusCode;
use serde::Deserialize;
use serde::Serialize;
use time::OffsetDateTime;
use tower_cookies::Cookie;
use tower_cookies::Cookies;

use crate::auth::types::SignUpRequest;
use crate::auth::types::TokensResponse;
use crate::http::json::Json;
use crate::AuthProvider;
use crate::PlatformError;
use crate::Result;
pub const COOKIE_NAME_REFRESH_TOKEN: &str = "refresh_token";

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LogInRequest {
    pub email: String,
    pub password: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RefreshTokenRequest {
    pub refresh_token: Option<String>,
}

fn set_refresh_token_cookie(cookies: &Cookies, refresh_token: &str, expires: OffsetDateTime) {
    let cookie = Cookie::build(COOKIE_NAME_REFRESH_TOKEN, refresh_token.to_owned())
        .expires(expires)
        .http_only(true)
        .finish();
    cookies.add(cookie);
}

#[debug_handler]
async fn sign_up(
    cookies: Cookies,
    Extension(provider): Extension<Arc<AuthProvider>>,
    Json(req): Json<SignUpRequest>,
) -> Result<(StatusCode, Json<TokensResponse>)> {
    let tokens = provider.sign_up(req).await?;
    set_refresh_token_cookie(
        &cookies,
        tokens.refresh_token.as_str(),
        OffsetDateTime::now_utc() + provider.refresh_token_duration.to_std().unwrap(),
    );

    Ok((StatusCode::CREATED, Json(tokens)))
}

async fn log_in(
    cookies: Cookies,
    Extension(provider): Extension<Arc<AuthProvider>>,
    Json(req): Json<LogInRequest>,
) -> Result<Json<TokensResponse>> {
    let tokens = provider
        .log_in(req.email.as_str(), req.password.as_str())
        .await?;
    set_refresh_token_cookie(
        &cookies,
        tokens.refresh_token.as_str(),
        OffsetDateTime::now_utc() + provider.refresh_token_duration.to_std().unwrap(),
    );

    Ok(Json(tokens))
}

async fn refresh_token(
    cookies: Cookies,
    Extension(provider): Extension<Arc<AuthProvider>>,
    Json(req): Json<RefreshTokenRequest>,
) -> Result<Json<TokensResponse>> {
    // read refresh token giving priority to cookies
    let refresh_token = if let Some(cookie) = cookies.get(COOKIE_NAME_REFRESH_TOKEN) {
        cookie.value().to_string()
    } else if let Some(cookie) = req.refresh_token {
        cookie
    } else {
        return Err(PlatformError::BadRequest(
            "refresh token doesn't not provided".to_string(),
        ));
    };

    let tokens = provider.refresh_token(refresh_token.as_str()).await?;
    set_refresh_token_cookie(
        &cookies,
        tokens.refresh_token.as_str(),
        OffsetDateTime::now_utc() + provider.refresh_token_duration.to_std().unwrap(),
    );

    Ok(Json(tokens))
}

pub fn attach_routes(router: Router, auth: Arc<AuthProvider>) -> Router {
    router.clone().nest(
        "/auth",
        router
            .route("/signup", post(sign_up))
            .route("/login", post(log_in))
            .route("/refresh-token", post(refresh_token))
            .layer(Extension(auth)),
    )
}
