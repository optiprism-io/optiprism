use std::sync::Arc;

use axum::extract::Extension;
use axum::routing;
use axum::Router;
use common::http::Json;
use crate::Context;
use crate::Result;
use crate::settings::{Settings, SettingsProvider};

async fn update(
    ctx: Context,
    Extension(provider): Extension<Arc<SettingsProvider>>,
    Json(request): Json<Settings>,
) -> Result<Json<Settings>> {
    Ok(Json(
        provider.set(ctx, request).await?,
    ))
}

async fn get(
    ctx: Context,
    Extension(provider): Extension<Arc<SettingsProvider>>,
) -> Result<Json<Settings>> {
    Ok(Json(provider.get(ctx).await?))
}


pub fn attach_routes(router: Router) -> Router {
    router.route("/api/v1/admin/settings", routing::put(update).get(get))
}
