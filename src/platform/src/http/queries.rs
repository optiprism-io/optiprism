use std::sync::Arc;

use axum::extract::Extension;
use axum::extract::Path;
use axum::extract::Query;
use axum::routing;
use axum::Router;
use serde_json::Value;

use crate::http::Json;
use crate::queries;
use crate::queries::event_segmentation::EventSegmentation;
use crate::queries::property_values::ListPropertyValuesRequest;
use crate::queries::QueryParams;
use crate::Context;
use crate::ListResponse;
use crate::QueryResponse;
use crate::Result;

async fn event_segmentation(
    ctx: Context,
    Extension(provider): Extension<Arc<dyn queries::Provider>>,
    Path((organization_id, project_id)): Path<(u64, u64)>,
    Query(query): Query<QueryParams>,
    Json(request): Json<EventSegmentation>,
) -> Result<Json<QueryResponse>> {
    Ok(Json(
        provider
            .event_segmentation(ctx, organization_id, project_id, request, query)
            .await?,
    ))
}

async fn property_values(
    ctx: Context,
    Extension(provider): Extension<Arc<dyn queries::Provider>>,
    Path((organization_id, project_id)): Path<(u64, u64)>,
    Json(request): Json<ListPropertyValuesRequest>,
) -> Result<Json<ListResponse<Value>>> {
    Ok(Json(
        provider
            .property_values(ctx, organization_id, project_id, request)
            .await?,
    ))
}

pub fn attach_routes(router: Router) -> Router {
    router.nest(
        "/organizations/:organization_id/projects/:project_id",
        Router::new()
            .route(
                "/queries/event-segmentation",
                routing::post(event_segmentation),
            )
            .route("/property-values", routing::post(property_values)),
    )
}
