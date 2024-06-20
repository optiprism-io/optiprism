use std::sync::Arc;

use arrow::array::Array;
use chrono::DateTime;
use chrono::Utc;
use common::rbac::ProjectPermission;
use metadata::MetadataProvider;
use query::context::Format;
use query::QueryProvider;
use serde_json::Value;
use metadata::properties::Type;

use crate::queries::funnel;
use crate::queries::funnel::FunnelRequest;
use crate::queries::group_records_search;
use crate::queries::group_records_search::GroupRecordsSearchRequest;
use crate::{Context, PropertyRef, QueryParams, QueryResponseFormat};
use crate::FunnelResponse;
use crate::FunnelStep;
use crate::FunnelStepData;
use crate::ListResponse;
use crate::QueryResponse;
use crate::Result;

pub struct Queries {
    query: Arc<QueryProvider>,
    md: Arc<MetadataProvider>,
}

impl Queries {
    pub fn new(query: Arc<QueryProvider>, md: Arc<MetadataProvider>) -> Self {
        Self { query, md }
    }
    pub async fn group_record_search(
        &self,
        ctx: Context,
        project_id: u64,
        req: GroupRecordsSearchRequest,
        query: QueryParams,
    ) -> Result<QueryResponse> {
        ctx.check_project_permission(
            ctx.organization_id,
            project_id,
            ProjectPermission::ExploreReports,
        )?;
        group_records_search::validate_request(&self.md, project_id, &req)?;
        let lreq = group_records_search::fix_request(req.into())?;
        let cur_time = match query.timestamp {
            None => Utc::now(),
            Some(ts_sec) => DateTime::from_naive_utc_and_offset(
                chrono::NaiveDateTime::from_timestamp_millis(ts_sec * 1000).unwrap(),
                Utc,
            ),
        };
        let ctx = query::Context {
            project_id,
            format: match &query.format {
                None => Format::Regular,
                Some(format) => match format {
                    QueryResponseFormat::Json => Format::Regular,
                    QueryResponseFormat::JsonCompact => Format::Compact,
                },
            },
            cur_time,
        };

        let mut data = self.query.group_records_search(ctx, lreq).await?;

        // do empty response so it will be [] instead of [[],[],[],...]
        if !data.columns.is_empty() && data.columns[0].data.is_empty() {
            data.columns = vec![];
        }
        let resp = match query.format {
            None => QueryResponse::columns_to_json(data.columns),
            Some(QueryResponseFormat::Json) => QueryResponse::columns_to_json(data.columns),
            Some(QueryResponseFormat::JsonCompact) => {
                QueryResponse::columns_to_json_compact(data.columns)
            }
        }?;

        Ok(resp)
    }


}
