use std::sync::Arc;

use arrow::array::Array;
use chrono::DateTime;
use chrono::Utc;
use common::rbac::ProjectPermission;
use metadata::MetadataProvider;
use query::context::Format;
use query::queries::funnel::StepData;
use query::QueryProvider;
use serde_json::Value;

use crate::queries::event_records_search;
use crate::queries::event_records_search::EventRecordsSearchRequest;
use crate::queries::event_segmentation;
use crate::queries::event_segmentation::EventSegmentation;
use crate::queries::funnel;
use crate::queries::funnel::Funnel;
use crate::queries::property_values::ListPropertyValuesRequest;
use crate::queries::QueryParams;
use crate::queries::QueryResponseFormat;
use crate::Context;
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

    pub async fn event_segmentation(
        &self,
        ctx: Context,
        project_id: u64,
        req: EventSegmentation,
        query: QueryParams,
    ) -> Result<QueryResponse> {
        ctx.check_project_permission(project_id, ProjectPermission::ExploreReports)?;
        event_segmentation::validate(&self.md, project_id, &req)?;
        let lreq = req.into();
        let lreq = event_segmentation::fix_types(&self.md, project_id, lreq)?;

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

        let mut data = self.query.event_segmentation(ctx, lreq).await?;

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

    pub async fn funnel(
        &self,
        ctx: Context,
        project_id: u64,
        req: Funnel,
        query: QueryParams,
    ) -> Result<FunnelResponse> {
        ctx.check_project_permission(project_id, ProjectPermission::ExploreReports)?;
        funnel::validate(&self.md, project_id, &req)?;

        let lreq = req.into();
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

        let mut qdata = self.query.funnel(ctx, lreq).await?;

        let steps = qdata
            .steps
            .iter()
            .map(|step| {
                let data = step
                    .data
                    .iter()
                    .map(|data| FunnelStepData {
                        groups: data.groups.clone(),
                        ts: data.ts.clone(),
                        total: data.total.clone(),
                        conversion_ratio: data.conversion_ratio.clone(),
                        avg_time_to_convert: data.avg_time_to_convert.clone(),
                        dropped_off: data.dropped_off.clone(),
                        drop_off_ratio: data.drop_off_ratio.clone(),
                        time_to_convert: data.time_to_convert.clone(),
                        time_to_convert_from_start: data.time_to_convert_from_start.clone(),
                    })
                    .collect::<Vec<_>>();
                FunnelStep {
                    step: step.step.clone(),
                    data,
                }
            })
            .collect::<Vec<_>>();
        let resp = FunnelResponse { steps };
        Ok(resp)
    }

    pub async fn event_record_search(
        &self,
        ctx: Context,
        project_id: u64,
        req: EventRecordsSearchRequest,
        query: QueryParams,
    ) -> Result<QueryResponse> {
        ctx.check_project_permission(project_id, ProjectPermission::ExploreReports)?;
        event_records_search::validate(&self.md, project_id, &req)?;
        let lreq = req.into();
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

        let mut data = self.query.event_records_search(ctx, lreq).await?;

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

    pub async fn property_values(
        &self,
        ctx: Context,
        project_id: u64,
        req: ListPropertyValuesRequest,
    ) -> Result<ListResponse<Value>> {
        ctx.check_project_permission(project_id, ProjectPermission::ExploreReports)?;

        let lreq = req.into();
        let result = self
            .query
            .property_values(query::Context::new(project_id), lreq)
            .await?;

        Ok(result.into())
    }
}
