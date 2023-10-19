use std::sync::Arc;

use axum::extract::Extension;
use axum::extract::Path;
use axum::routing;
use axum::Router;
use common::http::Json;

use crate::custom_properties;
use crate::custom_properties::CustomProperty;
use crate::Context;
use crate::ListResponse;
use crate::Result;

async fn list(
    ctx: Context,
    Extension(provider): Extension<Arc<dyn custom_properties::Provider>>,
    Path((organization_id, project_id)): Path<(u64, u64)>,
) -> Result<Json<ListResponse<CustomProperty>>> {
    Ok(Json(provider.list(ctx, organization_id, project_id).await?))
}

pub fn attach_routes(router: Router) -> Router {
    router.nest(
        "/organizations/:organization_id/projects/:project_id/schema/custom-properties",
        Router::new().route("/", routing::get(list)),
    )
}
