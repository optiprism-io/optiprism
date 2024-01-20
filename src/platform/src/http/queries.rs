use std::sync::Arc;

use axum::extract::Extension;
use axum::extract::Path;
use axum::extract::Query;
use axum::routing;
use axum::Router;
use common::http::Json;
use serde_json::Value;

use crate::queries::event_records_search::EventRecordsSearchRequest;
use crate::queries::event_segmentation::EventSegmentation;
use crate::queries::property_values::ListPropertyValuesRequest;
use crate::queries::Queries;
use crate::queries::QueryParams;
use crate::Context;
use crate::ListResponse;
use crate::QueryResponse;
use crate::Result;

async fn event_segmentation(
    ctx: Context,
    Extension(provider): Extension<Arc<Queries>>,
    Path(project_id): Path<u64>,
    Query(query): Query<QueryParams>,
    Json(request): Json<EventSegmentation>,
) -> Result<Json<QueryResponse>> {
    Ok(Json(
        provider
            .event_segmentation(ctx, project_id, request, query)
            .await?,
    ))
}

async fn property_values(
    ctx: Context,
    Extension(provider): Extension<Arc<Queries>>,
    Path(project_id): Path<u64>,
    Json(request): Json<ListPropertyValuesRequest>,
) -> Result<Json<ListResponse<Value>>> {
    Ok(Json(
        provider.property_values(ctx, project_id, request).await?,
    ))
}

async fn event_records_search(
    ctx: Context,
    Extension(provider): Extension<Arc<Queries>>,
    Path(project_id): Path<u64>,
    Query(query): Query<QueryParams>,
    Json(request): Json<EventRecordsSearchRequest>,
) -> Result<Json<QueryResponse>> {
    Ok(Json(
        provider
            .event_record_search(ctx, project_id, request, query)
            .await?,
    ))
}

pub fn attach_routes(router: Router) -> Router {
    router.nest(
        "/projects/:project_id",
        Router::new()
            .route(
                "/queries/event-segmentation",
                routing::post(event_segmentation),
            )
            .route("/property-values", routing::post(property_values))
            .route("/event-records/search", routing::post(event_records_search)),
    )
}
