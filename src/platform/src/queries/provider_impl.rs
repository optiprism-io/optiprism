use std::sync::Arc;

use axum::async_trait;
use common::rbac::ProjectPermission;
use query::context::Format;
use serde_json::Value;

use crate::queries::event_segmentation::EventSegmentation;
use crate::queries::property_values::ListPropertyValuesRequest;
use crate::queries::Provider;
use crate::queries::QueryParams;
use crate::queries::QueryResponseFormat;
use crate::Context;
use crate::DataTable;
use crate::ListResponse;
use crate::QueryResponse;
use crate::Result;

pub struct ProviderImpl {
    query: Arc<dyn query::Provider>,
}

impl ProviderImpl {
    pub fn new(query: Arc<dyn query::Provider>) -> Self {
        Self { query }
    }
}

#[async_trait]
impl Provider for ProviderImpl {
    async fn event_segmentation(
        &self,
        ctx: Context,
        organization_id: u64,
        project_id: u64,
        req: EventSegmentation,
        query: QueryParams,
    ) -> Result<QueryResponse> {
        ctx.check_project_permission(
            organization_id,
            project_id,
            ProjectPermission::ExploreReports,
        )?;
        let lreq = req.try_into()?;
        let ctx = query::Context {
            organization_id,
            project_id,
            format: match &query.format {
                None => Format::Regular,
                Some(format) => match format {
                    QueryResponseFormat::Json => Format::Regular,
                    QueryResponseFormat::JsonCompact => Format::Compact,
                    _ => unimplemented!(),
                },
            },
        };

        let data = self.query.event_segmentation(ctx, lreq).await?;

        let resp = match query.format {
            None => QueryResponse::try_new_json(data.columns),
            Some(QueryResponseFormat::Json) => QueryResponse::try_new_json(data.columns),
            Some(QueryResponseFormat::JsonCompact) => {
                QueryResponse::try_new_json_compact(data.columns)
            }
        }?;

        Ok(resp)
    }

    async fn property_values(
        &self,
        ctx: Context,
        organization_id: u64,
        project_id: u64,
        req: ListPropertyValuesRequest,
    ) -> Result<ListResponse<Value>> {
        ctx.check_project_permission(
            organization_id,
            project_id,
            ProjectPermission::ExploreReports,
        )?;

        let lreq = req.try_into()?;
        let result = self
            .query
            .property_values(query::Context::new(organization_id, project_id), lreq)
            .await?;

        result.try_into()
    }
}
