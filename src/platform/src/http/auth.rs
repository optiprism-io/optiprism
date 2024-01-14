use std::sync::Arc;

use axum::extract::Extension;
use axum::extract::Path;
use axum::routing::get;
use axum::routing::post;
use axum::routing::put;
use axum::Router;
use axum_macros::debug_handler;
use common::http::Json;
use reqwest::StatusCode;
use serde::Deserialize;
use serde::Serialize;
use time::OffsetDateTime;
use tower_cookies::Cookie;
use tower_cookies::Cookies;

use crate::accounts::Account;
use crate::auth;
use crate::auth::provider::Config;
use crate::auth::provider::LogInRequest;
use crate::auth::provider::SignUpRequest;
use crate::auth::provider::TokensResponse;
use crate::auth::provider::UpdateEmailRequest;
use crate::auth::provider::UpdateNameRequest;
use crate::auth::provider::UpdatePasswordRequest;
use crate::auth::Auth;
use crate::events::Event;
use crate::events::Events;
use crate::events::UpdateEventRequest;
use crate::Context;
use crate::PlatformError;
use crate::Result;

pub const COOKIE_NAME_REFRESH_TOKEN: &str = "refresh_token";

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
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
    Extension(provider): Extension<Arc<Auth>>,
    Extension(cfg): Extension<Config>,
    Json(req): Json<SignUpRequest>,
) -> Result<(StatusCode, Json<TokensResponse>)> {
    let tokens = provider.sign_up(req).await?;
    set_refresh_token_cookie(
        &cookies,
        tokens.refresh_token.as_str(),
        OffsetDateTime::now_utc() + cfg.refresh_token_duration.to_std().unwrap(),
    );

    Ok((StatusCode::CREATED, Json(tokens)))
}

async fn log_in(
    cookies: Cookies,
    Extension(provider): Extension<Arc<Auth>>,
    Extension(cfg): Extension<Config>,
    Json(req): Json<LogInRequest>,
) -> Result<Json<TokensResponse>> {
    let tokens = provider.log_in(req).await?;
    set_refresh_token_cookie(
        &cookies,
        tokens.refresh_token.as_str(),
        OffsetDateTime::now_utc() + cfg.refresh_token_duration.to_std().unwrap(),
    );

    Ok(Json(tokens))
}

async fn refresh_token(
    cookies: Cookies,
    Extension(provider): Extension<Arc<Auth>>,
    Extension(cfg): Extension<Config>,
    Json(req): Json<RefreshTokenRequest>,
) -> Result<Json<TokensResponse>> {
    // read refresh token giving priority to cookies
    let refresh_token = if let Some(cookie) = cookies.get(COOKIE_NAME_REFRESH_TOKEN) {
        cookie.value().to_string()
    } else if let Some(cookie) = req.refresh_token {
        cookie
    } else {
        return Err(PlatformError::BadRequest(
            "refresh token hasn't provided".to_string(),
        ));
    };

    let tokens = provider.refresh_token(refresh_token.as_str()).await?;
    set_refresh_token_cookie(
        &cookies,
        tokens.refresh_token.as_str(),
        OffsetDateTime::now_utc() + cfg.refresh_token_duration.to_std().unwrap(),
    );

    Ok(Json(tokens))
}

async fn get_profile(
    ctx: Context,
    Extension(provider): Extension<Arc<Auth>>,
) -> Result<Json<Account>> {
    Ok(Json(provider.get(ctx).await?))
}

async fn update_name(
    ctx: Context,
    Extension(provider): Extension<Arc<Auth>>,
    Json(request): Json<UpdateNameRequest>,
) -> Result<StatusCode> {
    provider.update_name(ctx, request.name).await?;

    Ok(StatusCode::OK)
}

async fn update_email(
    ctx: Context,
    Extension(provider): Extension<Arc<Auth>>,
    Json(request): Json<UpdateEmailRequest>,
) -> Result<Json<TokensResponse>> {
    let req = UpdateEmailRequest {
        email: request.email.to_string(),
        current_password: request.current_password.to_string(),
    };

    Ok(Json(provider.update_email(ctx, req).await?))
}

async fn update_password(
    ctx: Context,
    Extension(provider): Extension<Arc<Auth>>,
    Json(request): Json<UpdatePasswordRequest>,
) -> Result<Json<TokensResponse>> {
    let req = UpdatePasswordRequest {
        current_password: request.current_password.to_string(),
        new_password: request.new_password.to_string(),
    };

    Ok(Json(provider.update_password(ctx, req).await?))
}

pub fn attach_routes(router: Router) -> Router {
    router.nest(
        "/auth",
        Router::new()
            .route("/signup", post(sign_up))
            .route("/login", post(log_in))
            .route("/refresh-token", post(refresh_token))
            .route("/profile", get(get_profile))
            .route("/profile/name", put(update_name))
            .route("/profile/email", put(update_email))
            .route("/profile/password", put(update_password)),
    )
}
