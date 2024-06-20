use std::sync::Arc;

use axum::extract::Extension;
use axum::extract::Path;
use axum::extract::Query;
use axum::routing;
use axum::Router;
use common::http::Json;
use serde_json::Value;

use crate::queries::funnel::FunnelRequest;
use crate::queries::group_records_search::GroupRecordsSearchRequest;
use crate::queries::property_values::ListPropertyValuesRequest;
use crate::{Context, QueryParams};
use crate::event_segmentation::EventSegmentation;
use crate::FunnelResponse;
use crate::ListResponse;
use crate::queries::provider::Queries;
use crate::QueryResponse;
use crate::Result;

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

async fn group_records_search(
    ctx: Context,
    Extension(provider): Extension<Arc<Queries>>,
    Path(project_id): Path<u64>,
    Query(query): Query<QueryParams>,
    Json(request): Json<GroupRecordsSearchRequest>,
) -> Result<Json<QueryResponse>> {
    Ok(Json(
        provider
            .group_record_search(ctx, project_id, request, query)
            .await?,
    ))
}

pub fn attach_routes(router: Router) -> Router {
    router.nest(
        "/api/v1/projects/:project_id",
        Router::new()
            .route("/property-values", routing::post(property_values))
    )
}
