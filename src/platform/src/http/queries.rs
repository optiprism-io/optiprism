use axum::extract::{Extension, Path};
use axum::{routing, Json, Router};
use std::sync::Arc;

use crate::data_table::DataTable;
use crate::queries::event_segmentation::EventSegmentation;
use crate::queries::property_values;
use crate::queries::property_values::PropertyValues;
use crate::queries::provider::QueryProvider;
use crate::Context;
use crate::Result;

async fn event_segmentation(
    ctx: Context,
    Extension(provider): Extension<Arc<QueryProvider>>,
    Path((organization_id, project_id)): Path<(u64, u64)>,
    Json(request): Json<EventSegmentation>,
) -> Result<Json<DataTable>> {
    Ok(Json(
        provider
            .event_segmentation(ctx, organization_id, project_id, request)
            .await?,
    ))
}

async fn property_values(
    ctx: Context,
    Extension(provider): Extension<Arc<QueryProvider>>,
    Path((organization_id, project_id)): Path<(u64, u64)>,
    Json(request): Json<PropertyValues>,
) -> Result<Json<property_values::ListResponse>> {
    Ok(Json(
        provider
            .property_values(ctx, organization_id, project_id, request)
            .await?,
    ))
}

pub fn attach_routes(router: Router, prov: Arc<QueryProvider>) -> Router {
    router.clone().nest(
        "/organizations/:organization_id/projects/:project_id/queries",
        router
            .route("/event-segmentation", routing::post(event_segmentation))
            .route("/property-values", routing::post(property_values))
            .layer(Extension(prov)),
    )
}